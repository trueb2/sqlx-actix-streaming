use bytes::{Bytes, BytesMut};
use futures::{
    task::{Context, Poll},
    Stream,
};
use serde::Serialize;
pub use std::io::Write;
use std::fmt::Debug;
use std::{cell::RefCell, error::Error, pin::Pin, rc::Rc};

pub trait BytesWriter: Write {
    fn take(&mut self) -> Bytes;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;
}

#[derive(Clone)]
pub struct BufWriter {
    buf: Rc<RefCell<BytesMut>>,
}

impl BytesWriter for BufWriter {
    #[inline]
    fn take(&mut self) -> Bytes {
        (*self.buf).borrow_mut().split().freeze()
    }

    #[inline]
    fn len(&self) -> usize {
        self.buf.borrow().len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.buf.borrow().is_empty()
    }
}

impl Write for BufWriter {
    #[inline]
    fn write(&mut self, src: &[u8]) -> std::io::Result<usize> {
        (*self.buf).borrow_mut().extend_from_slice(src);
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
    /// Construct a self with a backing buffer size
    fn new(size: usize) -> Self;

    /// Set the prefix. For the json array, '[' by default.
    fn prefix() -> &'static [u8];

    /// Set the delimiter. For the json array element, ',' by default.
    fn delimiter() -> &'static [u8];

    /// Set the suffix. For the json array, ']' by default.
    fn suffix() -> &'static [u8];

    /// Get the internal byte buffer writer reference
    fn buffer(&mut self) -> &mut dyn BytesWriter;

    // append the configured prefix to the output buffer.
    #[inline]
    fn put_prefix(&mut self) {
        let _ = self.buffer().write(Self::prefix());
    }
    // append the configured delimiter to the output buffer.
    #[inline]
    fn put_delimiter(&mut self) {
        let _ = self.buffer().write(Self::delimiter());
    }
    // append the configured suffix to the output buffer.
    #[inline]
    fn put_suffix(&mut self) {
        let _ = self.buffer().write(Self::suffix());
    }

    // Serialize the self and/or the writer
    fn serialize<T>(&mut self, item: &T) -> Result<(), Box<dyn Error>>
    where
        T: Serialize + Debug;

    // Flush internals to the writer.
    fn flush(&mut self, writer: &mut dyn BytesWriter) -> Result<(), Box<dyn Error>>;

    // return the buffered output bytes.
    #[inline]
    fn bytes(&mut self) -> Bytes {
        self.buffer().take()
    }
}

pub struct JsonArrayContentType {
    buf: BufWriter,
}

impl ContentType for JsonArrayContentType {
    fn new(size: usize) -> Self {
        Self {
            buf: BufWriter {
                buf: Rc::new(RefCell::new(BytesMut::with_capacity(size))),
            },
        }
    }

    #[inline]
    fn buffer(&mut self) -> &mut dyn BytesWriter {
        &mut self.buf
    }

    #[inline]
    fn prefix() -> &'static [u8] {
        "[".as_bytes()
    }

    #[inline]
    fn delimiter() -> &'static [u8] {
        ",".as_bytes()
    }

    #[inline]
    fn suffix() -> &'static [u8] {
        "]".as_bytes()
    }

    #[inline]
    fn serialize<T>(&mut self, item: &T) -> Result<(), Box<dyn Error>>
    where
        T: Serialize + Debug,
    {
        println!("json: {:?}", item);
        serde_json::to_writer(&mut self.buf, item)?;
        Ok(())
    }

    #[inline]
    fn flush(&mut self, writer: &mut dyn BytesWriter) -> Result<(), Box<dyn Error>> {
        writer.flush()?;
        Ok(())
    }
}

pub struct CsvContentType {
    buf: BufWriter,
    writer: csv::Writer<BufWriter>,
}

impl ContentType for CsvContentType {
    #[inline]
    fn new(size: usize) -> Self {
        let buf = BufWriter {
            buf: Rc::new(RefCell::new(BytesMut::with_capacity(size))),
        };
        Self {
            buf: buf.clone(),
            writer: csv::Writer::from_writer(buf),
        }
    }

    #[inline]
    fn buffer(&mut self) -> &mut dyn BytesWriter {
        &mut self.buf
    }

    #[inline]
    fn prefix() -> &'static [u8] {
        "".as_bytes()
    }

    #[inline]
    fn delimiter() -> &'static [u8] {
        "".as_bytes()
    }

    #[inline]
    fn suffix() -> &'static [u8] {
        "".as_bytes()
    }

    #[inline]
    fn serialize<T>(&mut self, item: &T) -> Result<(), Box<dyn Error>>
    where
        T: Serialize + Debug,
    {
        println!("csv: {:?}", item);
        self.writer.serialize(item)?;
        let _ = self.writer.flush()?;
        Ok(())
    }

    #[inline]
    fn flush(&mut self, _writer: &mut dyn BytesWriter) -> Result<(), Box<dyn Error>> {
        self.writer.flush()?;
        Ok(())
    }
}

const BYTESTREAM_DEFAULT_ITEM_SIZE: usize = 128 * 1024;

pub type CsvStream<'a, InnerStream, InnerT, InnerError> =
    ByteStream<InnerStream, InnerT, InnerError, CsvContentType>;
pub type JsonStream<'a, InnerStream, InnerT, InnerError> =
    ByteStream<InnerStream, InnerT, InnerError, JsonArrayContentType>;

pub struct ByteStream<InnerStream, InnerT, InnerError, ContentTypeWriter>
where
    InnerStream: Stream<Item = Result<InnerT, InnerError>>,
    InnerError: std::error::Error,
    InnerT: Serialize,
    ContentTypeWriter: ContentType + Unpin,
{
    inner_stream: Pin<Box<InnerStream>>,
    state: State,
    writer: ContentTypeWriter,
}

impl<InnerStream, InnerT, InnerError, ContentTypeWriter>
    ByteStream<InnerStream, InnerT, InnerError, ContentTypeWriter>
where
    InnerStream: Stream<Item = Result<InnerT, InnerError>>,
    InnerError: std::error::Error,
    InnerT: Serialize + Debug,
    ContentTypeWriter: ContentType + Unpin,
{
    #[inline]
    pub fn new(inner_stream: InnerStream) -> Self {
        Self::with_size(inner_stream, BYTESTREAM_DEFAULT_ITEM_SIZE)
    }

    pub fn with_size(inner_stream: InnerStream, size: usize) -> Self {
        ByteStream {
            inner_stream: Box::pin(inner_stream),
            state: State::Unused,
            writer: ContentTypeWriter::new(size),
        }
    }

    // use the serializer to write one item to the buffer.
    #[inline]
    fn write_item(&mut self, record: InnerT) -> Result<(), Box<dyn Error>> {
        self.writer.serialize(&record)?;
        Ok(())
    }
}

impl<InnerStream, InnerT, InnerError, SerContentT> Stream
    for ByteStream<InnerStream, InnerT, InnerError, SerContentT>
where
    InnerStream: Stream<Item = Result<InnerT, InnerError>>,
    InnerError: std::error::Error + 'static,
    InnerT: Serialize + Debug,
    SerContentT: ContentType + Unpin,
{
    type Item = Result<Bytes, Box<dyn Error>>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        use State::*;
        match self.state {
            Unused => {
                self.state = Empty;
                self.writer.put_prefix();
            }
            Done => return Ready(None),
            _ => (),
        }
        loop {
            match self.inner_stream.as_mut().poll_next(cx) {
                Ready(Some(record)) => {
                    let Ok(record) = record else {
                        break Ready(Some(Err(record.err().unwrap().into())));
                    };
                    match self.state {
                        Empty => self.state = NonEmpty,
                        NonEmpty => self.writer.put_delimiter(),
                        _ => (),
                    };
                    if let Err(e) = self.write_item(record) {
                        break Ready(Some(Err(e)));
                    }
                    if self.writer.buffer().len() < BYTESTREAM_DEFAULT_ITEM_SIZE {
                        continue;
                    }
                    break Ready(Some(Ok(self.writer.bytes())));
                }
                Ready(None) => {
                    self.state = Done;
                    self.writer.put_suffix();
                    break Ready(Some(Ok(self.writer.bytes())));
                }
                Pending => {
                    if self.writer.buffer().is_empty() {
                        break Pending;
                    }
                    break Ready(Some(Ok(self.writer.bytes())));
                }
            }
        }
    }
}
