//! Binary artifact payload — bytes + MIME type, the canonical way media rides
//! the bus.
//!
//! The envelope payload is opaque (`Arc<dyn Any>`), so binary data has always
//! been *possible*; what was missing is a **shared representation** so that a
//! producer (a vision connector, an image fetch, a file upload) and a consumer
//! (a Telegram `send_photo`, an SMTP attachment, a disk sink) agree on "these
//! bytes, this format". [`Blob`] is that representation.
//!
//! It is deliberately minimal: bytes ([`bytes::Bytes`] — cheap to clone, shared
//! backing), a MIME `content_type`, and an optional `filename`. No codec is
//! registered for it (binary doesn't go through the JSON path); it travels as a
//! typed payload and is read by `payload_as::<Blob>()`.

use bytes::Bytes;

/// A binary artifact on the bus: bytes plus a MIME content type.
#[derive(Debug, Clone)]
pub struct Blob {
    bytes: Bytes,
    content_type: String,
    filename: Option<String>,
}

impl Blob {
    /// Construct from bytes and a MIME type (e.g. `image/jpeg`, `application/pdf`).
    pub fn new(bytes: impl Into<Bytes>, content_type: impl Into<String>) -> Self {
        Self {
            bytes: bytes.into(),
            content_type: content_type.into(),
            filename: None,
        }
    }

    /// Attach a suggested filename (used by connectors that need one, e.g. a
    /// document upload).
    pub fn with_filename(mut self, name: impl Into<String>) -> Self {
        self.filename = Some(name.into());
        self
    }

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    pub fn content_type(&self) -> &str {
        &self.content_type
    }

    pub fn filename(&self) -> Option<&str> {
        self.filename.as_deref()
    }

    /// `true` if the content type is an image (`image/*`).
    pub fn is_image(&self) -> bool {
        self.content_type.starts_with("image/")
    }

    /// `true` if the content type is audio (`audio/*`) — a voice message or a
    /// sound file, which a cogitator perceives by transcribing rather than by
    /// feeding it to the model directly.
    pub fn is_audio(&self) -> bool {
        self.content_type.starts_with("audio/")
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_basics() {
        let b = Blob::new(vec![1u8, 2, 3], "image/png").with_filename("octo.png");
        assert_eq!(b.len(), 3);
        assert_eq!(b.content_type(), "image/png");
        assert_eq!(b.filename(), Some("octo.png"));
        assert!(b.is_image());

        let doc = Blob::new(Bytes::from_static(b"hi"), "application/pdf");
        assert!(!doc.is_image());
        assert!(!doc.is_audio());
        assert_eq!(doc.filename(), None);
    }

    #[test]
    fn audio_is_distinguished_from_image() {
        let voice = Blob::new(vec![0u8; 4], "audio/ogg").with_filename("voice.ogg");
        assert!(voice.is_audio());
        assert!(!voice.is_image());
        assert!(!Blob::new(vec![1u8], "image/jpeg").is_audio());
    }
}
