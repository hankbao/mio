use std::io::{ErrorKind, Write, Read};
use std::net;
use std::time::Duration;

use mio::event::Evented;
use mio::net::{TcpListener, TcpStream};
use mio::{Poll, Events, Ready, PollOpt, Token};

#[test]
fn write_then_drop() {
    drop(::env_logger::init());

    let a = TcpListener::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = a.local_addr().unwrap();
    let mut s = TcpStream::connect(&addr).unwrap();

    let poll = Poll::new().unwrap();

    a.register(&poll,
               Token(1),
               Ready::readable(),
               PollOpt::edge()).unwrap();
    s.register(&poll,
               Token(3),
               Ready::empty(),
               PollOpt::edge()).unwrap();

    let mut events = Events::with_capacity(1024);
    while events.is_empty() {
        poll.poll(&mut events, None).unwrap();
    }
    assert_eq!(events.len(), 1);
    assert_eq!(events.get(0).unwrap().token(), Token(1));

    let mut s2 = a.accept().unwrap().0;

    s2.register(&poll,
                Token(2),
                Ready::writable(),
                PollOpt::edge()).unwrap();

    let mut events = Events::with_capacity(1024);
    while events.is_empty() {
        poll.poll(&mut events, None).unwrap();
    }
    assert_eq!(events.len(), 1);
    assert_eq!(events.get(0).unwrap().token(), Token(2));

    s2.write_all(&[1, 2, 3, 4]).unwrap();
    drop(s2);

    s.reregister(&poll,
                 Token(3),
                 Ready::readable(),
                 PollOpt::edge()).unwrap();
    let mut events = Events::with_capacity(1024);
    while events.is_empty() {
        poll.poll(&mut events, None).unwrap();
    }
    assert_eq!(events.len(), 1);
    assert_eq!(events.get(0).unwrap().token(), Token(3));

    let mut buf = [0; 10];
    assert_eq!(s.read(&mut buf).unwrap(), 4);
    assert_eq!(&buf[0..4], &[1, 2, 3, 4]);
}

/// A write that is still in flight when the stream is dropped must not keep
/// the socket alive: once the stream is gone (and, on Windows, the completion
/// port has had a chance to deliver the cancelled write's completion) the peer
/// has to observe the connection being closed instead of hanging forever.
#[test]
fn write_pending_then_drop_releases_socket() {
    drop(::env_logger::init());

    let a = TcpListener::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = a.local_addr().unwrap();

    let poll = Poll::new().unwrap();
    a.register(&poll,
               Token(1),
               Ready::readable(),
               PollOpt::edge()).unwrap();

    // The peer is a plain blocking socket which doesn't read anything until
    // the stream has been dropped, so that the writes below eventually block.
    let mut peer = net::TcpStream::connect(&addr).unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(10))).unwrap();

    let mut events = Events::with_capacity(16);
    'accept: loop {
        poll.poll(&mut events, None).unwrap();
        for event in events.iter() {
            if event.token() == Token(1) {
                break 'accept;
            }
        }
    }
    let mut s2 = a.accept().unwrap().0;
    s2.register(&poll,
                Token(2),
                Ready::writable(),
                PollOpt::edge()).unwrap();
    'writable: loop {
        poll.poll(&mut events, None).unwrap();
        for event in events.iter() {
            if event.token() == Token(2) {
                break 'writable;
            }
        }
    }

    // Fill the connection until a write is left pending (on Windows: an
    // overlapped `WSASend` in flight; on Unix: the kernel send buffer is full).
    let chunk = vec![0xAB; 64 * 1024];
    let mut written = 0;
    loop {
        match s2.write(&chunk) {
            Ok(n) => written += n,
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
            Err(e) => panic!("unexpected write error: {}", e),
        }
        assert!(written <= 256 * 1024 * 1024, "send never blocked");
    }

    drop(s2);

    // Let the completion port deliver the cancelled write's completion
    // (`write_done`), which returns the last reference to the socket. No
    // events are expected: the registration is gone.
    for _ in 0..10 {
        poll.poll(&mut events, Some(Duration::from_millis(100))).unwrap();
    }
    drop(poll);

    // The socket must be closed by now: the peer reads whatever the kernel had
    // already accepted and then sees EOF (or a reset), never a timeout.
    let mut buf = vec![0; 64 * 1024];
    let mut total = 0;
    loop {
        match peer.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(ref e) if e.kind() == ErrorKind::ConnectionReset ||
                          e.kind() == ErrorKind::ConnectionAborted => break,
            Err(e) => panic!("socket was not released: {}", e),
        }
    }
    assert!(total <= written);
}

#[test]
fn write_then_deregister() {
    drop(::env_logger::init());

    let a = TcpListener::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = a.local_addr().unwrap();
    let mut s = TcpStream::connect(&addr).unwrap();

    let poll = Poll::new().unwrap();

    a.register(&poll,
               Token(1),
               Ready::readable(),
               PollOpt::edge()).unwrap();
    s.register(&poll,
               Token(3),
               Ready::empty(),
               PollOpt::edge()).unwrap();

    let mut events = Events::with_capacity(1024);
    while events.is_empty() {
        poll.poll(&mut events, None).unwrap();
    }
    assert_eq!(events.len(), 1);
    assert_eq!(events.get(0).unwrap().token(), Token(1));

    let mut s2 = a.accept().unwrap().0;

    s2.register(&poll,
                Token(2),
                Ready::writable(),
                PollOpt::edge()).unwrap();

    let mut events = Events::with_capacity(1024);
    while events.is_empty() {
        poll.poll(&mut events, None).unwrap();
    }
    assert_eq!(events.len(), 1);
    assert_eq!(events.get(0).unwrap().token(), Token(2));

    s2.write_all(&[1, 2, 3, 4]).unwrap();
    s2.deregister(&poll).unwrap();

    s.reregister(&poll,
                 Token(3),
                 Ready::readable(),
                 PollOpt::edge()).unwrap();
    let mut events = Events::with_capacity(1024);
    while events.is_empty() {
        poll.poll(&mut events, None).unwrap();
    }
    assert_eq!(events.len(), 1);
    assert_eq!(events.get(0).unwrap().token(), Token(3));

    let mut buf = [0; 10];
    assert_eq!(s.read(&mut buf).unwrap(), 4);
    assert_eq!(&buf[0..4], &[1, 2, 3, 4]);
}
