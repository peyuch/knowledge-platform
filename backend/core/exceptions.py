"""Domain-specific exceptions."""

class KnowledgePlatformError(Exception):
    """Base exception for the application."""

class DocumentNotFound(KnowledgePlatformError):
    """Raised when a document ID does not exist."""

class DuplicateUploadError(KnowledgePlatformError):
    """Raised when the same file is uploaded by the same department."""

class TaskAlreadyCancelled(KnowledgePlatformError):
    """Raised when attempting to operate on a cancelled task."""

class MinerUError(KnowledgePlatformError):
    """Raised when MinerU API returns an error."""

class ASRError(KnowledgePlatformError):
    """Raised when all ASR providers fail."""

class CircuitBreakerOpenError(KnowledgePlatformError):
    """Raised when a circuit breaker is open."""
