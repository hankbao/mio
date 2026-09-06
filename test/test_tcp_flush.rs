//! `TcpStream::flush` on Windows.
//!
//! On Windows `write` copies the bytes into an internal buffer and reports
//! them as written as soon as the overlapped `WSASend` has been issued, and
//! dropping the stream cancels that send. `flush` is what tells a caller
//! whether the kernel has actually taken the bytes over yet: it must return
//! `WouldBlock` while the send is in flight and `Ok` once the writable event
//! that signals its completion has been delivered.

use std::io::{ErrorKind, Read, Write};
use std::net;
use std::thread;
use std::time::{Duration, Instant};

use net2::TcpStreamExt;
use mio::event::Evented;
use mio::net::TcpStream;
use mio::{Events, Poll, PollOpt, Ready, Token};

const WRITER: Token = Token(0);
const TIMEOUT: Duration = Duration::from_secs(30);

/// Connects a stream to a plain blocking peer and returns both once the
/// connect has completed.
///
/// The stream sends straight out of the caller's buffer (`SO_SNDBUF = 0`), so
/// a `WSASend` never completes synchronously and stays in flight until the
/// peer has acknowledged every byte. The peer's receive buffer is pinned to
/// `peer_rcvbuf` (which also disables autotuning) so that the tests control
/// how much the kernel can accept without the peer reading anything.
fn connect(poll: &Poll, events: &mut Events, peer_rcvbuf: usize)
           -> (TcpStream, net::TcpStream) {
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let stream = TcpStream::connect(&addr).unwrap();
    stream.register(poll, WRITER, Ready::writable(), PollOpt::edge()).unwrap();

    let (peer, _) = listener.accept().unwrap();
    peer.set_recv_buffer_size(peer_rcvbuf).unwrap();
    peer.set_read_timeout(Some(TIMEOUT)).unwrap();

    wait_writable(poll, events);
    stream.set_send_buffer_size(0).unwrap();
    assert_eq!(stream.send_buffer_size().unwrap(), 0);
    (stream, peer)
}

/// Polls until the stream reports a writable event.
fn wait_writable(poll: &Poll, events: &mut Events) {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let now = Instant::now();
        assert!(now < deadline, "no writable event within {:?}", TIMEOUT);
        poll.poll(events, Some(deadline - now)).unwrap();
        if events.iter().any(|e| e.token() == WRITER && e.readiness().is_writable()) {
            return;
        }
    }
}

/// Reads the peer to EOF and checks that it received exactly `expected`.
fn expect_exactly(peer: &mut net::TcpStream, expected: &[u8]) {
    let mut got = Vec::with_capacity(expected.len());
    peer.read_to_end(&mut got).unwrap();
    assert_eq!(got.len(), expected.len(),
               "peer received {} of {} bytes", got.len(), expected.len());
    assert!(got == expected, "peer received corrupted data");
}

/// Keeps polling while the peer's reader thread runs, then joins it.
///
/// Dropping the stream cancels the read that is always parked on a connected
/// stream; the socket is only closed (and the peer only sees EOF) once that
/// cancellation's completion has been dispatched by `Poll::poll`.
fn reap_until_done(poll: &Poll, events: &mut Events, reader: thread::JoinHandle<()>) {
    while !reader.is_finished() {
        poll.poll(events, Some(Duration::from_millis(10))).unwrap();
    }
    reader.join().unwrap();
}

/// The contract: `WouldBlock` while the send is in flight, `Ok` after the
/// writable event, and dropping the stream afterwards delivers everything.
#[test]
fn flush_reports_in_flight_write() {
    drop(::env_logger::init());

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(16);
    // A peer which can't absorb the whole write without reading.
    let (mut stream, mut peer) = connect(&poll, &mut events, 64 * 1024);

    let buf = vec![0x5A; 4 * 1024 * 1024];
    assert_eq!(stream.write(&buf).unwrap(), buf.len());

    // Nothing has been polled: the `WSASend` is still in flight.
    let err = stream.flush().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::WouldBlock);
    // Still in flight, the state must not have been touched.
    let err = stream.flush().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::WouldBlock);

    // Only now does the peer start reading, which lets the send complete.
    let expected = buf.clone();
    let reader = thread::spawn(move || expect_exactly(&mut peer, &expected));

    wait_writable(&poll, &mut events);
    stream.flush().unwrap();

    drop(stream);
    reap_until_done(&poll, &mut events, reader);
}

/// The `tokio::io::copy` scenario: write, flush until `Ok`, drop at once. The
/// peer must see every byte and then EOF, never a truncated stream.
///
/// This test fails without the `flush` change: `flush` used to return `Ok`
/// unconditionally, so the drop cancelled the `WSASend` while the kernel was
/// still consuming the buffer, and the peer got a reset instead of the data.
///
/// The peer only starts reading once the write has been issued. That is what
/// makes the failure deterministic: with the peer's receive window closed at
/// that point (64 KiB, nothing read yet) the kernel cannot have consumed more
/// than a fraction of the buffer by the time the stream is dropped. A peer
/// whose window is wide open from the start would let `WSASend` hand over
/// the whole buffer synchronously, leaving nothing for the cancel to discard,
/// while a window that is closed at write time only reopens once the peer
/// reads, so the send could never complete before the drop otherwise.
#[test]
fn flush_then_drop_delivers_everything() {
    drop(::env_logger::init());

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(16);
    let (mut stream, mut peer) = connect(&poll, &mut events, 64 * 1024);

    let buf = vec![0xA5; 8 * 1024 * 1024];
    assert_eq!(stream.write(&buf).unwrap(), buf.len());

    let expected = buf.clone();
    let reader = thread::spawn(move || expect_exactly(&mut peer, &expected));

    loop {
        match stream.flush() {
            Ok(()) => break,
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                wait_writable(&poll, &mut events);
            }
            Err(e) => panic!("flush failed: {}", e),
        }
    }
    drop(stream);

    reap_until_done(&poll, &mut events, reader);
}
