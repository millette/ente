class UnauthorizedError extends Error {}

class PassKeySessionNotVerifiedError extends Error {}

class PassKeySessionExpiredError extends Error {}

class SrpSetupNotCompleteError extends Error {}

class StorageLimitExceededError extends Error {}

class NoActiveSubscriptionError extends Error {}

// error when file size + current usage >= storage plan limit + buffer
class FileTooLargeForPlanError extends Error {}

class WiFiUnavailableError extends Error {}

class SilentlyCancelUploadsError extends Error {}

class SharingNotPermittedForFreeAccountsError extends Error {}

class InvalidFileError extends ArgumentError {
  final InvalidReason reason;

  InvalidFileError(String super.message, this.reason);

  @override
  String toString() {
    return 'InvalidFileError: $message (reason: $reason)';
  }
}

enum InvalidReason {
  assetDeleted,
  assetDeletedEvent,
  sourceFileMissing,
  livePhotoToImageTypeChanged,
  imageToLivePhotoTypeChanged,
  livePhotoVideoMissing,
  thumbnailMissing,
  tooLargeFile,
  unknown,
}

class BadMD5DigestError extends Error {
  final String message;
  BadMD5DigestError(this.message);

  @override
  String toString() => 'BadMD5DigestError: $message';
}

class EncSizeMismatchError extends Error {
  final String message;
  EncSizeMismatchError(this.message);

  @override
  String toString() => 'EncSizeMismatchError: $message';
}

class DuplicateUploadURLError extends Error {
  final DateTime firstUsedAt;
  final DateTime duplicateUsedAt;

  DuplicateUploadURLError({
    required this.firstUsedAt,
    required this.duplicateUsedAt,
  });

  @override
  String toString() =>
      'DuplicateUploadURLError: URL was already used at $firstUsedAt, duplicate attempt at $duplicateUsedAt';
}
