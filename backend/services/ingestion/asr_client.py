"""ASR client with primary (Aliyun) / fallback (Tencent) provider switching."""

import logging
from dataclasses import dataclass

from tenacity import retry, stop_after_attempt, wait_exponential

from core.exceptions import ASRError

logger = logging.getLogger(__name__)


@dataclass
class ASRResult:
    task_id: str
    status: str
    text: str | None = None


class AliyunASRClient:
    """Stub for Aliyun ASR API. Replace with actual SDK integration."""

    def __init__(self, access_key_id: str = "", access_key_secret: str = ""):
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret

    def submit(self, audio_url: str) -> str:
        logger.info(f"[Aliyun ASR] Submitting {audio_url}")
        return f"aliyun-task-{abs(hash(audio_url)) & 0xFFFF:04x}"

    def query(self, task_id: str) -> ASRResult:
        logger.info(f"[Aliyun ASR] Querying {task_id}")
        return ASRResult(task_id=task_id, status="done", text="[阿里云 ASR 识别结果占位]")

    def cancel(self, task_id: str) -> bool:
        return True


class TencentASRClient:
    """Stub for Tencent Cloud ASR. Auto-fallback when Aliyun fails."""

    def __init__(self, secret_id: str = "", secret_key: str = ""):
        self.secret_id = secret_id
        self.secret_key = secret_key

    def submit(self, audio_url: str) -> str:
        logger.info(f"[Tencent ASR] Submitting {audio_url}")
        return f"tencent-task-{abs(hash(audio_url)) & 0xFFFF:04x}"

    def query(self, task_id: str) -> ASRResult:
        logger.info(f"[Tencent ASR] Querying {task_id}")
        return ASRResult(task_id=task_id, status="done", text="[腾讯云 ASR 识别结果占位]")

    def cancel(self, task_id: str) -> bool:
        return True


class ASRClient:
    """ASR facade with primary -> fallback switching."""

    def __init__(self):
        self._primary = AliyunASRClient()
        self._fallback = TencentASRClient()
        self._using_fallback = False

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=5, max=30))
    def submit(self, audio_url: str) -> str:
        if self._using_fallback:
            try:
                return self._fallback.submit(audio_url)
            except Exception as e:
                raise ASRError(f"Both ASR providers failed: {e}")

        try:
            return self._primary.submit(audio_url)
        except Exception as primary_error:
            logger.warning(f"Primary ASR failed, falling back to tencent: {primary_error}")
            self._using_fallback = True
            try:
                return self._fallback.submit(audio_url)
            except Exception as fb_error:
                raise ASRError(f"Both ASR providers failed. Primary: {primary_error}, Fallback: {fb_error}")

    def query(self, task_id: str) -> ASRResult:
        if task_id.startswith("tencent-"):
            return self._fallback.query(task_id)
        return self._primary.query(task_id)

    def cancel(self, task_id: str) -> bool:
        if task_id.startswith("tencent-"):
            return self._fallback.cancel(task_id)
        return self._primary.cancel(task_id)

    def reset_provider(self) -> None:
        self._using_fallback = False


_asr_client: ASRClient | None = None


def get_asr_client() -> ASRClient:
    global _asr_client
    if _asr_client is None:
        _asr_client = ASRClient()
    return _asr_client
