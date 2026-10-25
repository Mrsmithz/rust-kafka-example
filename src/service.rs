use thiserror::Error;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MessageSummary {
    pub(crate) byte_count: usize,
    pub(crate) word_count: usize,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum ServiceError {
    #[error("message payload is missing")]
    MissingPayload,
    #[error("message payload is not valid UTF-8")]
    InvalidUtf8,
    #[error("message payload is empty after trimming whitespace")]
    EmptyPayload,
}

pub(crate) fn process_payload(payload: Option<&[u8]>) -> Result<MessageSummary, ServiceError> {
    let payload = payload.ok_or(ServiceError::MissingPayload)?;
    let text = std::str::from_utf8(payload).map_err(|_| ServiceError::InvalidUtf8)?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(ServiceError::EmptyPayload);
    }
    Ok(MessageSummary {
        byte_count: trimmed.len(),
        word_count: trimmed.split_whitespace().count(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summarizes_ascii_payload() -> Result<(), ServiceError> {
        let summary = process_payload(Some(b"hello world"))?;
        assert_eq!(summary.byte_count, 11);
        assert_eq!(summary.word_count, 2);
        Ok(())
    }

    #[test]
    fn trims_unicode_whitespace_before_counting() -> Result<(), ServiceError> {
        let summary = process_payload(Some(" \u{2003}café  世界\n".as_bytes()))?;
        assert_eq!(summary.byte_count, "café  世界".len());
        assert_eq!(summary.word_count, 2);
        Ok(())
    }

    #[test]
    fn rejects_missing_payload_without_content_in_error() {
        match process_payload(None) {
            Err(error) => {
                assert_eq!(error, ServiceError::MissingPayload);
                assert!(!error.to_string().contains("payload bytes"));
            }
            Ok(_) => panic!("missing payload must fail"),
        }
    }

    #[test]
    fn rejects_empty_payload() {
        assert_eq!(process_payload(Some(b"")), Err(ServiceError::EmptyPayload));
    }

    #[test]
    fn rejects_whitespace_only_payload() {
        assert_eq!(
            process_payload(Some(" \u{2003}\n".as_bytes())),
            Err(ServiceError::EmptyPayload)
        );
    }

    #[test]
    fn rejects_invalid_utf8_without_lossy_decoding() {
        match process_payload(Some(&[0x66, 0x80, 0x67])) {
            Err(error) => {
                assert_eq!(error, ServiceError::InvalidUtf8);
                assert!(!error.to_string().contains("f�g"));
            }
            Ok(_) => panic!("invalid UTF-8 must fail"),
        }
    }
}
