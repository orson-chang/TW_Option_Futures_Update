import argparse
import os
import time
from pathlib import Path

import requests

try:
    from linebot.v3.messaging import (
        ApiClient,
        Configuration,
        ImageMessage,
        MessagingApi,
        PushMessageRequest,
        TextMessage,
    )
except Exception:
    ApiClient = None
    Configuration = None
    ImageMessage = None
    MessagingApi = None
    PushMessageRequest = None
    TextMessage = None

LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"
CLOUDINARY_UPLOAD_API = "https://api.cloudinary.com/v1_1/{cloud_name}/image/upload"

SUPPORTED_TRANSPORTS = ("requests", "sdk")

UPLOAD_RETRIES = 3
PUSH_RETRIES = 3
READY_RETRIES = 10
READY_DELAY_SEC = 1.0
DEFAULT_PREVIEW_WIDTH = 360


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing env var: {name}")
    return value


def _get_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _validate_to_id(to_id: str) -> str:
    if len(to_id) < 10:
        raise RuntimeError("Invalid LINE to-id. Expected userId/groupId/roomId.")
    return to_id


def _line_headers() -> dict[str, str]:
    token = _require_env("LINE_CHANNEL_ACCESS_TOKEN")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _line_to_id() -> str:
    return _validate_to_id(_require_env("LINE_TO_USER_ID"))


def _push_messages_requests(messages: list[dict]) -> None:
    payload = {
        "to": _line_to_id(),
        "messages": messages,
    }
    response = requests.post(
        LINE_PUSH_URL,
        headers=_line_headers(),
        json=payload,
        timeout=(6, 20),
    )
    if response.status_code != 200:
        raise RuntimeError(f"LINE push failed: {response.status_code} {response.text}")


def _build_sdk_message(message: dict):
    msg_type = str(message.get("type", "")).strip().lower()
    if msg_type == "text":
        return TextMessage(type="text", text=str(message.get("text", "")))
    if msg_type == "image":
        return ImageMessage(
            type="image",
            original_content_url=str(message.get("originalContentUrl", "")).strip(),
            preview_image_url=str(message.get("previewImageUrl", "")).strip(),
        )
    raise RuntimeError(f"Unsupported message type for SDK transport: {msg_type}")


def _push_messages_sdk(messages: list[dict]) -> None:
    if not all((ApiClient, Configuration, MessagingApi, PushMessageRequest, TextMessage, ImageMessage)):
        raise RuntimeError("line-bot-sdk is not installed. Install with: pip install line-bot-sdk")

    sdk_messages = [_build_sdk_message(msg) for msg in messages]
    configuration = Configuration(access_token=_require_env("LINE_CHANNEL_ACCESS_TOKEN"))
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        push_request = PushMessageRequest(to=_line_to_id(), messages=sdk_messages)
        line_bot_api.push_message(push_request)


def _push_messages(messages: list[dict], transport: str = "requests") -> None:
    transport_key = transport.strip().lower()
    if transport_key == "requests":
        _push_messages_requests(messages)
        return
    if transport_key == "sdk":
        _push_messages_sdk(messages)
        return
    raise RuntimeError(f"Unsupported transport: {transport}. Use one of {SUPPORTED_TRANSPORTS}.")


def _ensure_file_exists(file_path: Path) -> None:
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")


def push_line_message(text: str, transport: str = "requests") -> None:
    _push_messages([{"type": "text", "text": text}], transport=transport)


def upload_image_cloudinary(file_path: Path, preview_width: int = DEFAULT_PREVIEW_WIDTH) -> tuple[str, str]:
    _ensure_file_exists(file_path)
    cloud_name = _require_env("CLOUDINARY_CLOUD_NAME")
    upload_preset = _require_env("CLOUDINARY_UPLOAD_PRESET")
    folder = _get_env("CLOUDINARY_UPLOAD_FOLDER")

    upload_url = CLOUDINARY_UPLOAD_API.format(cloud_name=cloud_name)
    data = {"upload_preset": upload_preset}
    if folder:
        data["folder"] = folder

    with file_path.open("rb") as fp:
        response = requests.post(
            upload_url,
            data=data,
            files={"file": (file_path.name, fp)},
            timeout=(8, 45),
        )
    if response.status_code != 200:
        body = (response.text or "")[:500]
        raise RuntimeError(f"Cloudinary upload failed: {response.status_code} {body}")

    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Cloudinary upload failed: unexpected payload {type(payload)!r}")

    original_url = str(payload.get("secure_url", "")).strip()
    public_id = str(payload.get("public_id", "")).strip()
    ext = str(payload.get("format", "")).strip()
    version = str(payload.get("version", "")).strip()

    if not original_url.startswith("https://"):
        raise RuntimeError(f"Cloudinary upload failed: invalid secure_url {original_url!r}")

    if not public_id:
        return original_url, original_url

    preview_width = max(120, int(preview_width))
    public_with_ext = f"{public_id}.{ext}" if ext else public_id
    version_segment = f"v{version}/" if version else ""
    preview_transform = f"f_auto,q_auto:eco,w_{preview_width},c_limit"
    preview_url = (
        f"https://res.cloudinary.com/{cloud_name}/image/upload/"
        f"{preview_transform}/{version_segment}{public_with_ext}"
    )
    return original_url, preview_url


def upload_image_cloudinary_with_retry(
    file_path: Path,
    preview_width: int = DEFAULT_PREVIEW_WIDTH,
    retries: int = UPLOAD_RETRIES,
) -> tuple[str, str]:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return upload_image_cloudinary(file_path=file_path, preview_width=preview_width)
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1.2 * attempt)
    raise RuntimeError(f"Cloudinary upload failed after {retries} retries: {last_error}") from last_error


def wait_until_image_ready(
    image_url: str,
    retries: int = READY_RETRIES,
    delay_sec: float = READY_DELAY_SEC,
) -> None:
    headers = {"User-Agent": "Mozilla/5.0"}
    last_state = ""
    for _ in range(retries):
        try:
            with requests.get(
                image_url,
                headers=headers,
                timeout=(6, 15),
                stream=True,
            ) as response:
                content_type = (response.headers.get("Content-Type") or "").lower()
                first_chunk = b""
                if response.status_code == 200 and "image" in content_type:
                    first_chunk = next(response.iter_content(chunk_size=4096), b"")
                    if first_chunk:
                        return
                last_state = (
                    f"status={response.status_code}, "
                    f"ctype={content_type}, "
                    f"first_chunk={len(first_chunk)}"
                )
        except Exception as exc:
            last_state = str(exc)
        time.sleep(delay_sec)
    raise RuntimeError(f"Image URL not ready: {image_url}; {last_state}")


def push_line_image_url(
    image_url: str,
    text: str | None = None,
    preview_image_url: str | None = None,
    transport: str = "requests",
) -> None:
    preview_url = preview_image_url or image_url
    messages: list[dict] = []
    if text:
        messages.append({"type": "text", "text": text})
    messages.append(
        {
            "type": "image",
            "originalContentUrl": image_url,
            "previewImageUrl": preview_url,
        }
    )
    _push_messages(messages, transport=transport)


def push_line_image_url_with_retry(
    image_url: str,
    text: str | None = None,
    preview_image_url: str | None = None,
    transport: str = "requests",
    retries: int = PUSH_RETRIES,
) -> None:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            push_line_image_url(
                image_url=image_url,
                text=text,
                preview_image_url=preview_image_url,
                transport=transport,
            )
            return
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1.2 * attempt)
    raise RuntimeError(f"LINE image push failed after {retries} retries: {last_error}") from last_error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Push LINE text or image message with Cloudinary only.",
    )
    parser.add_argument(
        "--text",
        default="LINE push test.",
        help="text message to send",
    )
    parser.add_argument(
        "--file",
        help="local image path to upload then send",
    )
    parser.add_argument(
        "--no-text",
        action="store_true",
        help="when used with --file, send image only",
    )
    parser.add_argument(
        "--ready-retries",
        type=int,
        default=READY_RETRIES,
        help="max checks before sending image (default: 10)",
    )
    parser.add_argument(
        "--ready-delay-sec",
        type=float,
        default=READY_DELAY_SEC,
        help="seconds between image ready checks (default: 1.0)",
    )
    parser.add_argument(
        "--preview-width",
        type=int,
        default=DEFAULT_PREVIEW_WIDTH,
        help="preview width for Cloudinary transform (default: 360)",
    )
    parser.add_argument(
        "--transport",
        choices=list(SUPPORTED_TRANSPORTS),
        default="requests",
        help="LINE push transport (default: requests)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.file:
        file_path = Path(args.file).expanduser().resolve()
        start = time.perf_counter()
        original_url, preview_url = upload_image_cloudinary_with_retry(
            file_path=file_path,
            preview_width=args.preview_width,
        )
        wait_until_image_ready(
            image_url=preview_url,
            retries=max(1, args.ready_retries),
            delay_sec=max(0.2, args.ready_delay_sec),
        )
        wait_until_image_ready(
            image_url=original_url,
            retries=max(1, args.ready_retries),
            delay_sec=max(0.2, args.ready_delay_sec),
        )
        text = None if args.no_text else file_path.name
        push_line_image_url_with_retry(
            image_url=original_url,
            text=text,
            preview_image_url=preview_url,
            transport=args.transport,
        )
        elapsed = time.perf_counter() - start
        print(
            "sent image (cloudinary): "
            f"original={original_url} preview={preview_url} "
            f"(elapsed={elapsed:.2f}s)"
        )
        return 0

    push_line_message(args.text, transport=args.transport)
    print("sent text")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
