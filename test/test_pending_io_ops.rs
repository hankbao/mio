//! `Poll::pending_io_ops` on Windows.
//!
//! Every overlapped operation mio issues loans a reference to the socket to
//! the completion port, and that reference is only returned when `Poll::poll`
//! dispatches the completion -- also for an operation that was cancelled by
//! dropping the socket. The counter tells an event loop that is shutting
//! down how many of those are still outstanding, i.e. how long it has to
//! keep polling before the sockets are actually gone.

use std::io::{self, ErrorKind, Read, Write};
use std::net;
use std::thread;
use std::time::{Duration, Instant};

use kernel32;
use net2::TcpStreamExt;
use mio::event::Evented;
use mio::net::{TcpListener, TcpStream, UdpSocket};
use mio::{Events, Poll, PollOpt, Ready, Token};

const TIMEOUT: Duration = Duration::from_secs(30);

fn handle_count() -> u32 {
    let mut count = 0;
    let ok = unsafe {
        kernel32::GetProcessHandleCount(kernel32::GetCurrentProcess(), &mut count)
    };
    assert!(ok != 0, "GetProcessHandleCount: {}", io::Error::last_os_error());
    count
}

/// Polls until `token` reports `ready`.
fn wait_for(poll: &Poll, events: &mut Events, token: Token, ready: Ready) {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let now = Instant::now();
        assert!(now < deadline, "no {:?} event for {:?} within {:?}",
                ready, token, TIMEOUT);
        poll.poll(events, Some(deadline - now)).unwrap();
        if events.iter().any(|e| e.token() == token && e.readiness().contains(ready)) {
            return;
        }
    }
}

/// Polls until every in-flight operation has been reaped.
fn reap_all(poll: &Poll, events: &mut Events) {
    let deadline = Instant::now() + TIMEOUT;
    while poll.pending_io_ops() != 0 {
        assert!(Instant::now() < deadline, "{} operations still in flight after {:?}",
                poll.pending_io_ops(), TIMEOUT);
        poll.poll(events, Some(Duration::from_millis(100))).unwrap();
    }
}

/// Reads the peer until it observes the mio side being closed.
fn expect_closed(peer: &mut net::TcpStream) {
    peer.set_read_timeout(Some(TIMEOUT)).unwrap();
    let mut buf = vec![0; 64 * 1024];
    loop {
        match peer.read(&mut buf) {
            Ok(0) => return,
            Ok(_) => {}
            Err(ref e) if e.kind() == ErrorKind::ConnectionReset ||
                          e.kind() == ErrorKind::ConnectionAborted => return,
            Err(e) => panic!("socket was not released: {}", e),
        }
    }
}

/// A stream connected to a blocking peer accepted from `listener`, registered
/// for both directions, with its connect completed: from then on the 0-byte
/// read that parks the stream is in flight. The peer's receive buffer is
/// pinned to 64 KiB so that a large send cannot complete while the peer is
/// not reading.
fn connect(poll: &Poll, events: &mut Events, listener: &net::TcpListener,
           token: Token) -> (TcpStream, net::TcpStream) {
    let stream = TcpStream::connect(&listener.local_addr().unwrap()).unwrap();
    stream.register(poll, token, Ready::readable() | Ready::writable(),
                    PollOpt::edge()).unwrap();
    let (peer, _) = listener.accept().unwrap();
    peer.set_recv_buffer_size(64 * 1024).unwrap();
    wait_for(poll, events, token, Ready::writable());
    (stream, peer)
}

/// Parks a send on the stream: with `SO_SNDBUF = 0` the `WSASend` stays in
/// flight until the peer, which does not read, has acknowledged everything.
fn park_send(stream: &mut TcpStream, buf: &[u8]) {
    stream.set_send_buffer_size(0).unwrap();
    assert_eq!(stream.write(buf).unwrap(), buf.len());
    assert_eq!(stream.flush().unwrap_err().kind(), ErrorKind::WouldBlock);
}

/// Every kind of operation counts exactly once while it is in flight, and
/// the counter only returns to zero once all the cancellations issued by
/// dropping the sockets have been dispatched -- at which point the sockets
/// are really gone.
fn exercise(poll: &Poll, events: &mut Events) {
    const READERS: usize = 4;
    const WRITERS: usize = 3;

    assert_eq!(poll.pending_io_ops(), 0);
    let peer_listener = net::TcpListener::bind("127.0.0.1:0").unwrap();

    // A listener nobody connects to: one accept in flight.
    let listener = TcpListener::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
    let listener_addr = listener.local_addr().unwrap();
    listener.register(poll, Token(1), Ready::readable(), PollOpt::edge()).unwrap();
    assert_eq!(poll.pending_io_ops(), 1);

    // A UDP socket: one recv in flight.
    let udp = UdpSocket::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
    let udp_addr = udp.local_addr().unwrap();
    udp.register(poll, Token(2), Ready::readable(), PollOpt::edge()).unwrap();
    assert_eq!(poll.pending_io_ops(), 2);

    // Connected streams: the connect is one operation until it has been
    // dispatched, after which the read that parks the stream replaces it.
    let mut streams = Vec::new();
    let mut peers = Vec::new();
    for i in 0..READERS + WRITERS {
        let (stream, peer) = connect(poll, events, &peer_listener, Token(10 + i));
        streams.push(stream);
        peers.push(peer);
        assert_eq!(poll.pending_io_ops(), 2 + i + 1);
    }

    // Park a send on some of them on top of the read.
    let buf = vec![0xAB; 2 * 1024 * 1024];
    for stream in &mut streams[READERS..] {
        park_send(stream, &buf);
    }
    assert_eq!(poll.pending_io_ops(), 2 + READERS + 2 * WRITERS);

    // A connect that has been issued but not dispatched yet.
    let late = TcpStream::connect(&peer_listener.local_addr().unwrap()).unwrap();
    late.register(poll, Token(3), Ready::writable(), PollOpt::edge()).unwrap();
    let (late_peer, _) = peer_listener.accept().unwrap();
    peers.push(late_peer);
    let total = 2 + READERS + 2 * WRITERS + 1;
    assert_eq!(poll.pending_io_ops(), total);

    // Dropping everything cancels all of it, but each cancellation is a
    // completion that still has to be dispatched.
    drop(streams);
    drop(late);
    drop(listener);
    drop(udp);
    assert_eq!(poll.pending_io_ops(), total);
    reap_all(poll, events);

    // Nothing holds the sockets open any more: the peers see them closed and
    // the listener's and the UDP socket's ports are free again.
    for mut peer in peers {
        expect_closed(&mut peer);
    }
    drop(peer_listener);
    net::TcpListener::bind(listener_addr).unwrap();
    net::UdpSocket::bind(udp_addr).unwrap();
}

#[test]
fn counts_every_kind_of_operation() {
    drop(::env_logger::init());

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(64);

    // A round must not leave a single handle behind, and the process handle
    // count is the only place where that shows up. It is shared with the
    // other tests running in parallel in this process though, several of
    // which leak sockets for good: dropping a `Poll` while a cancellation is
    // still in flight never reaps it. Waiting for the count to come back
    // down would therefore wait forever, so measure a window of our own
    // instead and repeat the round until one of them falls into a quiet one
    // -- a round that leaks nothing cannot end above where it started. The
    // first round is never that one anyway: it still pays for the one-time
    // initialisation (Winsock itself, the `AcceptEx`/`ConnectEx` extension
    // lookups) whose handles stay around for the rest of the process.
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let before = handle_count();
        exercise(&poll, &mut events);
        let after = handle_count();
        if after <= before {
            break;
        }
        assert!(Instant::now() < deadline,
                "every round grew the process handle count, last {} to {}",
                before, after);
        thread::sleep(Duration::from_millis(50));
    }
}

/// A completion that arrives after its owner has been dropped still counts
/// until it is dispatched, and dispatching it is what releases the socket.
#[test]
fn cancelled_operations_are_reaped_after_drop() {
    drop(::env_logger::init());

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(16);
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();

    let (mut stream, mut peer) = connect(&poll, &mut events, &listener, Token(0));
    assert_eq!(poll.pending_io_ops(), 1);
    park_send(&mut stream, &vec![0xCD; 2 * 1024 * 1024]);
    assert_eq!(poll.pending_io_ops(), 2);

    drop(stream);
    assert_eq!(poll.pending_io_ops(), 2);
    reap_all(&poll, &mut events);
    expect_closed(&mut peer);
}

/// Completions that carry an error are reaped like any other, whether the
/// owner is gone or still around to receive the error.
#[test]
fn error_completions_are_reaped() {
    drop(::env_logger::init());

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(16);

    // A refused connect: the completion carries the error, which the owner
    // then gets from `take_error`.
    let refused = net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let stream = TcpStream::connect(&refused).unwrap();
    stream.register(&poll, Token(0), Ready::readable() | Ready::writable(),
                    PollOpt::edge()).unwrap();
    assert_eq!(poll.pending_io_ops(), 1);
    wait_for(&poll, &mut events, Token(0), Ready::writable());
    assert_eq!(poll.pending_io_ops(), 0);
    let err = stream.take_error().unwrap().expect("connect should have failed");
    assert_eq!(err.kind(), ErrorKind::ConnectionRefused);
    drop(stream);
    assert_eq!(poll.pending_io_ops(), 0);

    // A UDP recv hit by an ICMP "port unreachable": a datagram sent to a port
    // nobody listens on makes the parked recv complete with `WSAECONNRESET`.
    // The send is in flight as well until its completion is dispatched.
    let udp = UdpSocket::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
    udp.register(&poll, Token(1), Ready::readable() | Ready::writable(),
                 PollOpt::edge()).unwrap();
    assert_eq!(poll.pending_io_ops(), 1);
    let nobody = net::UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    assert_eq!(udp.send_to(b"ping", &nobody).unwrap(), 4);
    assert_eq!(poll.pending_io_ops(), 2);
    wait_for(&poll, &mut events, Token(1), Ready::readable());
    reap_all(&poll, &mut events);
    drop(udp);
    assert_eq!(poll.pending_io_ops(), 0);

    // A parked send whose peer resets the connection. Both the send and the
    // parked read complete while the owner is alive. Windows reports the
    // send either as failed with `WSAECONNRESET`, which `flush` then hands
    // out, or as successful (the stack claims the whole buffer and discards
    // it), in which case the next write trips over the reset instead.
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let (mut stream, peer) = connect(&poll, &mut events, &listener, Token(2));
    park_send(&mut stream, &vec![0xEF; 2 * 1024 * 1024]);
    assert_eq!(poll.pending_io_ops(), 2);
    TcpStreamExt::set_linger(&peer, Some(Duration::from_secs(0))).unwrap();
    drop(peer);
    reap_all(&poll, &mut events);
    let mut attempts = 0;
    let err = loop {
        attempts += 1;
        assert!(attempts <= 8, "the reset never surfaced");
        match stream.flush() {
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => reap_all(&poll, &mut events),
            Err(e) => break e,
            Ok(()) => match stream.write(b"x") {
                Ok(_) => {}
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => reap_all(&poll, &mut events),
                Err(e) => break e,
            },
        }
    };
    assert!(err.kind() == ErrorKind::ConnectionReset ||
            err.kind() == ErrorKind::ConnectionAborted, "unexpected error: {}", err);
    // An error is handed out once; afterwards `flush` is clean again.
    stream.flush().unwrap();
    reap_all(&poll, &mut events);
    drop(stream);
    assert_eq!(poll.pending_io_ops(), 0);
}
