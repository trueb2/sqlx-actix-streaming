use bytes::{Bytes, BytesMut};
use futures::{
    task::{Context, Poll},
    Stream, TryStream,
};
pub use std::io::Write;
use std::{pin::Pin, marker::PhantomData};

pub trait BytesWriter: Write {
    fn take(&mut self) -> Bytes;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;
}

pub struct BufWriter {
    buf: BytesMut,
}

impl BytesWriter for BufWriter {
    #[inline]
    fn take(&mut self) -> Bytes {
        self.buf.split().freeze()
    }

    #[inline]
    fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
       self.buf.is_empty() 
    }
}

impl Write for BufWriter {
    #[inline]
    fn write(&mut self, src: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(src);
        Ok(src.len())
    }
    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub enum State {
    /// Unused is the initial state of a new instance. Change to Empty
    /// upon self.poll_next().
    Unused,
    /// Empty means self.poll_next() has been called at least once.
    /// Change to NonEmpty when inner_stream.poll_next() returns
    /// Ready(Ok(item).
    Empty,
    /// NonEmpty means inner_stream.poll_next() has returned a
    /// Ready(Ok(item) at least once. Change to Done when
    /// inner_stream.poll_next() returns Ready(None).
    NonEmpty,
    /// Done means inner_stream.poll_next() has returned Ready(None).
    Done,
}

pub trait ContentType {
        /// Set the prefix. For the json array, '[' by default.
         fn prefix() -> &'static str;

        /// Set the delimiter. For the json array element, ',' by default.
         fn delimiter() -> &'static str;

        /// Set the suffix. For the json array, ']' by default.
         fn suffix() -> &'static str;
}

pub struct JsonArrayContentType;
impl ContentType for JsonArrayContentType {
    #[inline]
    fn prefix() -> &'static str {
        "["
    }

    #[inline]
    fn delimiter() -> &'static str {
        ","
    }

    #[inline]
    fn suffix() -> &'static str {
        "]"
    }
}

pub struct CsvContentType;
impl ContentType for CsvContentType {
    #[inline]
    fn prefix() -> &'static str {
        ""
    }

    #[inline]
    fn delimiter() -> &'static str {
        ""
    }

    #[inline]
    fn suffix() -> &'static str {
        ""
    }
}



const BYTESTREAM_DEFAULT_ITEM_SIZE: usize = 4096;

pub struct ByteStream<InnerStream, InnerError, Serializer, SerializerContent, OuterError>
where
    InnerError: std::error::Error,
    InnerStream: TryStream<Error = InnerError>,
    OuterError: From<InnerError> + std::error::Error,
    Serializer:
        FnMut(&mut dyn BytesWriter, &<InnerStream as TryStream>::Ok) -> Result<(), OuterError> + Unpin,
    SerializerContent: ContentType + Unpin,
{
    inner_stream: Pin<Box<InnerStream>>,
    serializer: Serializer,
    state: State,
    prefix: &'static [u8],
    delimiter: &'static [u8],
    suffix: &'static [u8],
    buf: Box<dyn BytesWriter>,
    _serializer_content: PhantomData<SerializerContent>,
}

impl<InnerStream, InnerError, Serializer, SerializerContent, OuterError>
    ByteStream<InnerStream, InnerError, Serializer, SerializerContent, OuterError>
where
    InnerError: std::error::Error,
    InnerStream: TryStream<Error = InnerError>,
    OuterError: From<InnerError> + std::error::Error,
    Serializer:
        FnMut(&mut dyn BytesWriter, &<InnerStream as TryStream>::Ok) -> Result<(), OuterError> + Unpin,
    SerializerContent: ContentType + Unpin,

{
    #[inline]
    pub fn new(_content_type: PhantomData<SerializerContent>, inner_stream: InnerStream, serializer: Serializer) -> Self {
        Self::with_size(inner_stream, serializer, BYTESTREAM_DEFAULT_ITEM_SIZE)
    }
    pub fn with_size(inner_stream: InnerStream, serializer: Serializer, size: usize) -> Self {
        Self {
            inner_stream: Box::pin(inner_stream),
            serializer,
            state: State::Unused,
            prefix: SerializerContent::prefix().as_bytes(),
            delimiter: SerializerContent::delimiter().as_bytes(),
            suffix: SerializerContent::suffix().as_bytes(),
            buf: Box::new(BufWriter { buf: BytesMut::with_capacity(size) }),
            _serializer_content: PhantomData,
        }
    }

    // append the configured prefix to the output buffer.
    #[inline]
    fn put_prefix(&mut self) {
        let _ = self.buf.write(&self.prefix);
    }
    // append the configured delimiter to the output buffer.
    #[inline]
    fn put_delimiter(&mut self) {
        let _ = self.buf.write(&self.delimiter);
    }
    // append the configured suffix to the output buffer.
    #[inline]
    fn put_suffix(&mut self) {
        let _ = self.buf.write(&self.suffix);
    }
    // return the buffered output bytes.
    #[inline]
    fn bytes(&mut self) -> Bytes {
        self.buf.take()
    }
    // use the serializer to write one item to the buffer.
    #[inline]
    fn write_item(&mut self, record: &<InnerStream as TryStream>::Ok) -> Result<(), OuterError> {
        (self.serializer)(&mut *self.buf, record)
    }
}

impl<InnerStream, InnerError, Serializer, SerializerContent, OuterError> Stream
    for ByteStream<InnerStream, InnerError, Serializer, SerializerContent, OuterError>
where
    InnerError: std::error::Error,
    InnerStream: TryStream<Error = InnerError>,
    OuterError: From<InnerError> + std::error::Error,
    Serializer:
        FnMut(&mut dyn BytesWriter, &<InnerStream as TryStream>::Ok) -> Result<(), OuterError> + Unpin,
    SerializerContent: ContentType + Unpin,
{
    type Item = Result<Bytes, OuterError>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        use State::*;
        match self.state {
            Unused => {
                self.state = Empty;
                self.put_prefix();
            }
            Done => return Ready(None),
            _ => (),
        }
        loop {
            match self.inner_stream.as_mut().try_poll_next(cx) {
                Ready(Some(Ok(record))) => {
                    match self.state {
                        Empty => self.state = NonEmpty,
                        NonEmpty => self.put_delimiter(),
                        _ => (),
                    };
                    if let Err(e) = self.write_item(&record) {
                        break Ready(Some(Err(e)));
                    }
                    if self.buf.len() < BYTESTREAM_DEFAULT_ITEM_SIZE {
                        continue;
                    }
                    break Ready(Some(Ok(self.bytes())));
                }
                Ready(Some(Err(e))) => {
                    break Ready(Some(Err(OuterError::from(e))));
                }
                Ready(None) => {
                    self.state = Done;
                    self.put_suffix();
                    break Ready(Some(Ok(self.bytes())));
                }
                Pending => {
                    if self.buf.is_empty() {
                        break Pending;
                    }
                    break Ready(Some(Ok(self.bytes())));
                }
            }
        }
    }
}
