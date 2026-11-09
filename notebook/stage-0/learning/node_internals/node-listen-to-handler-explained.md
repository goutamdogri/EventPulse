# What really happens from `server.listen(8000)` to your request handler

A step-by-step story of one Node HTTP server, from the moment you call `listen` until the response leaves your process. It corrects the places where the usual mental model goes wrong, and it was checked with real experiments (see section 11).

The server used throughout is your example:

```js
import http from "node:http";

const myRequestHandler = async (req, res) => {
  if (req.method === "GET" && req.url === "/health") {
    console.log("GET /health", new Date().toLocaleTimeString());
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ status: "ok" }));
  } else {
    res.writeHead(404);
    res.end();
  }
};

const server = http.createServer(myRequestHandler);
server.listen(8000, () => console.log("Server running on :8000"));
```

---

## Contents

1. [The story in one page](#1-the-story-in-one-page)
2. [Who does what](#2-who-does-what)
3. [Step 1: `listen()` creates a listening socket](#3-step-1-listen-creates-a-listening-socket)
4. [Step 2: a client connects (the kernel does this alone)](#4-step-2-a-client-connects-the-kernel-does-this-alone)
5. [Step 3: Node accepts the connection](#5-step-3-node-accepts-the-connection)
6. [Step 4: request bytes arrive and wait in the kernel](#6-step-4-request-bytes-arrive-and-wait-in-the-kernel)
7. [Step 5: when does Node read? (the event loop)](#7-step-5-when-does-node-read-the-event-loop)
8. [Step 6: parsing and calling your handler](#8-step-6-parsing-and-calling-your-handler)
9. [Step 7: the response goes out](#9-step-7-the-response-goes-out)
10. [How the OS tracks everything, and how you can look at it](#10-how-the-os-tracks-everything-and-how-you-can-look-at-it)
11. [Experiments I ran (and you can repeat)](#11-experiments-i-ran-and-you-can-repeat)
12. [Your assumptions, checked one by one](#12-your-assumptions-checked-one-by-one)
13. [Notes on your sample code](#13-notes-on-your-sample-code)

---

## 1. The story in one page

```
 your JS            Node C++ / libuv                 Linux kernel                         network
 -------            ----------------                 ------------                         -------
 server.listen() -> socket, bind, listen  --------->  LISTENING SOCKET on port 8000
                    epoll: "watch this fd"            (has an accept queue inside)

                                                      client SYN  ----------------------> arrives
                                                      kernel finishes the 3-way handshake
                                                      (Node is NOT involved)
                                                      connection waits in ACCEPT QUEUE

                    poll phase: listening fd ready
                    accept4()  ------------------->   removes it from the queue,
                                                      returns a NEW fd (new socket,
                                                      same local port 8000)
 'connection' <---- wrap fd, attach HTTP parser,
                    watch the new fd in epoll

                                                      request bytes arrive
                                                      -> receive buffer of THAT socket
                                                      (Recv-Q grows, Node not told yet)
                    poll phase: connection fd ready
                    read() copies bytes to Node  <--  bytes leave the kernel buffer
                    HTTP parser runs
 myRequestHandler <-- called directly, synchronously, from the parser (no queue)
 res.end() --------> write() ----------------------->  bytes go into the send buffer
                                                      kernel sends them (and handles ACKs)
```

The five facts that matter most:

1. `listen()` does not "open a port" in a vague sense. It creates a **listening socket** in the kernel, and the kernel then answers connection attempts for that port **by itself**.
2. The TCP connection is **built by the kernel** before Node knows about it. Node's `accept()` only picks up connections that already exist.
3. A request waits as raw bytes in the **kernel's receive buffer for that connection**. Node has no queue of pending requests.
4. Node reads when its event loop reaches the **poll phase**. It does not wait for any queue to be empty.
5. Your handler is **not** added to the microtask queue. The HTTP parser calls it **directly**, inside the same step in which the bytes were read.

---

## 2. Who does what

| Layer | What it is | Its job in this story |
|---|---|---|
| Your JavaScript | `myRequestHandler`, `server.listen` | Business logic. Runs on the single main thread. |
| Node's JS library | `node:http`, `node:net` | Builds `req` and `res`, emits events (`connection`, `request`). |
| Node's C++ layer | `TCPWrap`, the HTTP parser (llhttp) | Bridges JS to libuv, parses HTTP bytes into requests. |
| libuv | The event loop library | Owns the epoll instance, calls `accept4`, `read`, `write`. Decides when callbacks run. |
| Linux kernel | TCP/IP stack, socket objects, epoll | Completes TCP handshakes, owns the accept queue and the buffers, moves packets, signals readiness. |
| Network card or loopback | | Delivers the packets. (For `localhost` traffic the kernel loops packets back internally.) |

So when you ask "who handles it, Node or the OS?", the answer is: **the OS handles the connection; Node handles the conversation**. The kernel establishes and buffers connections. Node decides when to take them and what to do with them.

---

## 3. Step 1: `listen()` creates a listening socket

### What happens, layer by layer

1. **JavaScript:** `server.listen(8000)` is `net.Server.listen` (an `http.Server` is a `net.Server`). It creates a TCP handle object and asks it to bind and listen.
2. **Node C++ / libuv:** this turns into system calls. In order, the typical sequence is:

```
socket(AF_INET6, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, ...) = fd    create a socket
setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, 1)                               allow quick restart
bind(fd, address "::" or "0.0.0.0", port 8000)                            attach it to port 8000
listen(fd, 511)                                                           switch it to LISTEN state
epoll_ctl(epfd, EPOLL_CTL_ADD, fd, EPOLLIN)                               "tell me when it is readable"
```

(The exact order and options vary slightly between Node versions. IPv6 dual-stack `::` is tried first when `host` is omitted, which is why `ss` shows `*:8000`. This is the typical shape, not a byte-exact trace.)

### What each call means

| Call | Meaning |
|---|---|
| `socket` | Asks the kernel to create a socket object. It returns a **file descriptor (fd)**, a small number that is your process's handle to it. `SOCK_NONBLOCK` means reads and accepts never freeze the thread. |
| `bind` | Associates the socket with an address and port. The kernel refuses if another socket already owns that port (`EADDRINUSE`). |
| `listen(fd, 511)` | Switches the socket to the **LISTEN state** and sets the **backlog** (the maximum length of the accept queue; Node's default is 511, and the kernel caps it at `net.core.somaxconn`). |
| `epoll_ctl ADD` | Registers the fd with Node's epoll instance, so the event loop is told when a connection is ready to be accepted. |

### Why "a running process" alone does not accept connections

A port is not owned by a process. It is owned by a **socket in LISTEN state** that is bound to it.

- When a SYN packet (the first step of a connection) arrives for port 8000, the kernel looks for a listening socket bound to that port.
- If there is none, the kernel replies with a reset, and the client sees **"connection refused"**. This happens even if a process is running.
- If there is one, the kernel starts the handshake for it.

So what `server.listen(8000)` special does is **create that listening socket**. That is the whole difference between a process that accepts connections and one that does not.

### The two queues inside a listening socket

```
 incoming SYN
      |
      v
 [ SYN queue ]      half-open connections (SYN received, SYN-ACK sent, waiting for the final ACK)
      |  handshake completes
      v
 [ ACCEPT queue ]   fully established connections waiting for the application to call accept()
      |  accept()
      v
 your process gets a new fd
```

Both live in **kernel memory**, inside the listening socket's data structures. They are not files. You inspect them with tools (section 10).

---

## 4. Step 2: a client connects (the kernel does this alone)

When a client calls `connect()` to port 8000:

1. Client kernel sends **SYN**.
2. Server kernel (the listening socket) replies **SYN-ACK** and puts the half-open connection in the SYN queue.
3. Client kernel sends **ACK**. The connection is now **ESTABLISHED** from the kernel's point of view, and it moves to the accept queue.

**Node is not involved in any of these steps.** It does not even need to be running its event loop. The kernel completes handshakes until the accept queue is full. From the client's side, `connect()` has succeeded, even though no application has seen the connection yet.

### What if Node is busy and the queue fills up?

Linux lets the accept queue hold about **backlog + 1** connections. New handshake attempts beyond that are, by default, **silently dropped** (`tcp_abort_on_overflow` is 0). The client does not get an error; it retransmits its SYN with growing delays (about 1 s, then 2 s more, then 4 s more, and so on). That is why k6 showed connect times of 7 s, 11 s and 19 s, and why the kernel counters `ListenOverflows` and `ListenDrops` increase.

I verified this: with backlog 4 and Node blocked for 6 seconds, the kernel showed an accept queue of **5**, with more clients stuck waiting (section 11, experiment 1).

---

## 5. Step 3: Node accepts the connection

### When?

When the event loop reaches the **poll phase** and epoll reports that the **listening** fd is readable. For a listening socket, "readable" means "the accept queue is not empty".

If Node's thread is busy (for example running a long callback), this does not happen until that callback finishes. Meanwhile the kernel keeps queueing connections (until the queue is full). In my experiment, Node accepted all 8 waiting clients within about 55 ms of the moment it became free.

### What happens

1. libuv calls **`accept4()`** on the listening fd (it may accept several waiting connections in one pass).
2. The kernel removes one established connection from the accept queue and returns a **new fd** for a **new socket object**.
3. libuv calls back into Node's C++ layer, which wraps the new fd in a `TCPWrap` handle, and Node emits the **`'connection'`** event with a `net.Socket`.
4. The `http.Server` listener attaches an **HTTP parser** to that socket and starts reading.
5. libuv registers the new fd with epoll (`epoll_ctl ADD`), so it will be told when request bytes arrive.

### Important details

- **`accept()` does not create the connection.** It only collects a connection that already exists and gives your process a handle for it.
- **The new socket has the same local port, 8000.** The listening socket keeps existing, still bound to 8000, still accepting. The connection socket is told apart from others by its **4-tuple**: (client IP, client port, server IP, server port). In my experiment the listener and two accepted connections all showed local port 8102, with different client ports.
- There is **one listening socket** and **one connection socket per client**. Fds are only meaningful inside your process (fd 21 here is not fd 21 in another process).

---

## 6. Step 4: request bytes arrive and wait in the kernel

When the client sends `GET /health HTTP/1.1 ...`:

1. The packet arrives (on loopback, the kernel hands it straight back in).
2. The kernel's network stack (running as a softirq, not in your process) finds the right socket by the 4-tuple and puts the bytes in **that socket's receive buffer**.
3. The kernel marks that fd as readable, which wakes anyone waiting in `epoll_wait`.

Until Node reads them, the bytes **stay in the kernel**. This is the "Recv-Q" of that connection in `ss` output.

I verified it: I blocked Node's event loop right after it accepted a connection, sent a 33-byte request, and the kernel showed **Recv-Q = 33** for that connection. When Node became free, it read exactly those 33 bytes (experiment 3).

If a client sends faster than Node reads, the receive buffer fills, TCP shrinks the "window" it advertises, and the client is told to slow down. That is how TCP provides backpressure with no help from Node.

---

## 7. Step 5: when does Node read? (the event loop)

This is the part most often mis-pictured. There is **no single task queue** that must empty before Node reads a socket. Instead the event loop goes around a fixed set of phases:

```
        +-----------------------------------------------------------+
        |                                                           |
        v                                                           |
   [ timers ]  -> [ pending callbacks ] -> [ poll ] -> [ check ] -> [ close callbacks ]
   due setTimeout   deferred system         I/O: sockets    setImmediate    socket 'close'
   / setInterval    callbacks               are read HERE
```

- **Poll phase:** libuv calls `epoll_wait` and gets back the list of fds that are ready (up to a batch limit). For each one, it runs that fd's callback: accept for the listening fd, read for a connection fd.
- **If other work is waiting** (a due timer, a `setImmediate`), the poll phase does not sleep. If nothing is pending, the loop sleeps inside `epoll_wait` and wakes the instant data arrives.

### Where microtasks and `process.nextTick` fit

After **each** callback that libuv runs (each accept, each read), Node drains two small queues before moving on:

1. the `process.nextTick` queue,
2. the **microtask queue** (promise continuations, `queueMicrotask`).

So the order inside one loop turn looks like this, for three ready sockets A, B and C:

```
poll phase:
   read A -> parse -> handler(A) runs -> [drain nextTick + microtasks]
   read B -> parse -> handler(B) runs -> [drain nextTick + microtasks]
   read C -> parse -> handler(C) runs -> [drain nextTick + microtasks]
then: check phase, close callbacks, timers, ... and around again
```

### The answers to "when?"

| Your question | Answer |
|---|---|
| Does Node read the socket only when the queue is empty? | No. It reads in the poll phase **regardless** of what else is waiting, as long as the loop gets there. |
| Can it read while callbacks or promises are pending? | Yes. Pending microtasks are drained after each callback, and then the loop proceeds to the next ready fd. |
| Can a request be read while another handler is running? | **Never.** There is one thread, and a running callback is never interrupted. Requests wait in the kernel until the current callback finishes. |
| What can delay reading? | A **long callback** (a slow synchronous function), or a **microtask / `nextTick` chain that keeps re-adding itself**, which keeps the loop from ever reaching the poll phase (starvation). |

This is also the mechanism behind everything you measured: when each request takes about 36 µs of main-thread time, 23,000 requests per second leaves the thread about 83% busy, and requests increasingly wait in the kernel buffers before the loop reaches them.

---

## 8. Step 6: parsing and calling your handler

### What actually happens

In the normal case, Node's HTTP server hands the connection's stream directly to the C++ HTTP parser (`parser.consume(socket._handle)` in `node:_http_server`). So for each read:

```
libuv read() copies bytes from the kernel buffer into Node's buffer
   -> HTTP parser (llhttp) parses the bytes
        -> parserOnHeadersComplete  (JS callback)
             -> parserOnIncoming    (builds req and res)
                  -> server.emit('request', req, res)
                       -> myRequestHandler(req, res)     <- called right here
```

I printed the call stack inside the handler to prove it:

```
at Server.<anonymous>            <- your handler
at Server.emit
at parserOnIncoming
at HTTPParser.parserOnHeadersComplete
```

There is no microtask, no queue and no `setImmediate` between the parser and your function. **The handler runs synchronously inside the parser callback.**

### Your function is `async`. Does that change anything?

- Calling an `async` function runs its body **synchronously up to the first `await`**. Your handler has no `await`, so the entire body runs inside that call.
- The function returns a Promise, which Node ignores.
- If your handler did `await something`, the code after the `await` would run later as a **microtask continuation**. That is the only place microtasks enter the picture, and it is a continuation of your function, not the delivery of the request.
- I confirmed the ordering: a promise callback queued inside the handler runs only **after** the handler returns.
- One side effect: if an `async` handler throws, the Promise rejects, which becomes an unhandled rejection (a crash by default in modern Node). A plain function would throw into the parser instead.

### What about the request body?

For `POST` requests, the headers are parsed first and the handler is called right away. The body arrives as `data` events on `req`, possibly across several reads. Your handler can start before the whole body is received.

### Where multiple requests on one connection fit

If one read contains two pipelined requests, the parser extracts and dispatches them in order. Normal HTTP/1.1 clients (browsers, k6) wait for the response before sending the next request on a connection, so this is rare.

---

## 9. Step 7: the response goes out

1. `res.writeHead(...)` prepares the status line and headers.
2. `res.end(body)` turns headers and body into bytes and writes them to the socket.
3. Node first tries a **synchronous, non-blocking `write()`** (or `writev`) to the kernel. If the kernel's send buffer has room, the bytes are copied in right there, **inside your handler call**, in a single system call in the simple case. Only if the buffer cannot take everything does Node queue the rest and wait until the fd is writable.
4. After the call returns, the kernel handles the rest by itself: segmentation, transmission, retransmission and ACKs. The bytes sit in the socket's **send buffer** until the client acknowledges them (this is "Send-Q" on that connection).

### After the response

- The connection **stays open** (HTTP keep-alive). My test response included `Keep-Alive: timeout=5`: Node closes a connection that sits idle for 5 seconds (`server.keepAliveTimeout`, default 5000 ms).
- The next request on the same connection skips steps 2 and 3 entirely, with no new handshake and no new accept. That is why your healthy k6 runs showed `http_req_connecting` of almost zero.
- When one side closes, a FIN exchange runs, and the side that closed first keeps a **TIME_WAIT** entry for 60 seconds.

---

## 10. How the OS tracks everything, and how you can look at it

### The tracking chain

```
process's fd table            kernel objects
  fd 21  ------------------>  listening socket   (port 8000, LISTEN, contains the accept queue)
  fd 22  ------------------>  connection socket A (4-tuple, receive buffer, send buffer)
  fd 23  ------------------>  connection socket B
  fd 10  ------------------>  epoll instance     (interest list: 21, 22, 23, ...)
```

- Every process has its own **fd table**: small integers that point to open kernel objects (files, sockets, pipes, epoll instances).
- A socket is a kernel object. For listening sockets the kernel keeps a table keyed by address and port; for established connections, a table keyed by the 4-tuple.
- **The queue is not a file.** The fd is only a handle. The accept queue and the buffers live in kernel memory inside the socket object. You see them through tools.

### Commands

| What you want | Command |
|---|---|
| Your process's fds, with sockets shown as `socket:[inode]` | `ls -l /proc/<pid>/fd` |
| Listening sockets with their queue and owning process | `ss -ltnp 'sport = :8000'` |
| Established connections on the port, with buffers | `ss -tn 'sport = :8000'` |
| Same data in raw form | `cat /proc/net/tcp` |
| Which fds Node has registered in epoll | `cat /proc/<pid>/fdinfo/<epoll-fd>` |
| Everything a process has open | `lsof -p <pid>` |
| Drop counters from queue overflow | `nstat -az TcpExtListenOverflows TcpExtListenDrops` |

### What Recv-Q and Send-Q mean (they mean different things on different sockets)

| Socket type | Recv-Q | Send-Q |
|---|---|---|
| **Listening** socket | Connections currently **waiting in the accept queue** | The **maximum** accept queue length (the backlog, for example 511) |
| **Established** connection | Bytes that **arrived but your process has not read yet** | Bytes **sent but not yet acknowledged** by the other side |

On the first row, in the raw `/proc/net/tcp` file, the number after the colon in the `tx_queue:rx_queue` column is the accept queue length for a listening socket.

---

## 11. Experiments I ran (and you can repeat)

I ran these with Node v22 on Linux. The sandbox had no `ss` or `strace`, so I read `/proc/net/tcp`, which is the same data `ss` uses. Counts and port numbers will differ on your machine; the behaviour should not.

### Experiment 1: the kernel accepts connections while Node is blocked

A server with backlog 4 that blocks its own event loop for 6 seconds right after `listen`, with 8 clients connecting meanwhile.

```js
const net = require('net');
const srv = net.createServer(() => {});
srv.listen(8101, '127.0.0.1', 4, () => {
  const end = Date.now() + 6000;
  while (Date.now() < end) {}          // Node cannot call accept() here
});
srv.on('connection', () => console.log('accepted', Date.now()));
```
Start 8 clients (for example `(sleep 8 | nc 127.0.0.1 8101) &` eight times), then look at the kernel while Node is blocked.

Result while Node was blocked:

```
LISTEN socket: accept-queue length = 5      <- backlog 4, queue holds backlog + 1
ESTABLISHED entries: 10                     <- 5 connections, seen from both ends
SYN_SENT: 3                                 <- clients whose handshake was dropped (queue full)
```

When the loop became free, Node accepted all 8 within about 55 ms (the 3 stuck clients connected after retransmitting their SYN). Conclusion: **the handshake and the queue are the kernel's work; Node only collects.**

### Experiment 2: fds, sockets and epoll

Your server on port 8102, one keep-alive client connected. Results:

```
before any client:   fd 21 -> socket:[1079]            (the listening socket)
after two clients:   fd 22 -> socket:[1090], fd 23 -> socket:[1116]   (one new fd per connection)

state          local-port  remote-port  inode
LISTEN         8102        0            1079        <- listening socket
ESTABLISHED    8102        59428        1090        <- connection socket 1 (same local port)
ESTABLISHED    8102        59444        1116        <- connection socket 2 (same local port)

epoll interest list (fdinfo): fd 21 (listener), fd 22, fd 23, ...  events = EPOLLIN | EPOLLERR | EPOLLHUP
idle main thread: blocked in the kernel function ep_poll (waiting in epoll_wait)
```

Conclusion: one listening socket plus one socket per connection, all using local port 8102; each fd is registered in epoll; an idle Node main thread sleeps in `epoll_wait`.

### Experiment 3: request bytes wait in the kernel receive buffer

Node accepts a connection, then blocks for 4 seconds. A client sends a 33-byte HTTP request during that time.

```
while Node was blocked:  Recv-Q = 33 bytes
after Node was free:     "Node finally read 33 bytes from the socket"
```

Conclusion: **an unread request is bytes in the kernel's buffer, not an object in Node.**

### Experiment 4: where the handler runs

Printing `new Error().stack` inside the handler gave `handler <- Server.emit <- parserOnIncoming <- HTTPParser.parserOnHeadersComplete`, with no microtask or queue in between, and a `Promise.resolve().then(...)` created inside the handler ran after the handler returned.

### Try the system calls yourself

On your machine, trace the real sequence (this produces a lot of lines; look for `socket`, `bind`, `listen`, `accept4`):

```
strace -f -e trace=network,epoll_ctl,read,write,writev node server.js
```
Send one request with `curl localhost:8000/health` in another terminal. You should see, in order: `socket`, `bind`, `listen(…, 511)`, `epoll_ctl(ADD)`, then later `accept4`, `epoll_ctl(ADD)` for the new fd, `read`, and a `write`/`writev` for the response. (I could not run `strace` in my environment, so I have not reproduced exact output lines here.)

---

## 12. Your assumptions, checked one by one

| Your assumption | Verdict | Correction |
|---|---|---|
| `server.listen(8000)` is C++ code making OS calls to open the port | **Mostly right** | The path is JS `net` code, then Node's C++ `TCPWrap`, then libuv, which makes the syscalls (`socket`, `bind`, `listen`, `epoll_ctl`). "Opening a port" really means creating a bound, listening socket. |
| The OS creates a socket that listens for new connections and establishes them | **Right** | The kernel completes TCP handshakes by itself, with no help from Node. |
| The OS tracks everything with fds | **Right, with one addition** | The fd is your process's handle. The kernel also keeps its own tables (by port for listeners, by 4-tuple for connections) and the queues inside the socket objects. |
| Can I show the file that holds the queue? | **There is no such file** | The queue is kernel memory inside the socket. See it with `ss`, `/proc/net/tcp`, `/proc/<pid>/fd`. |
| An established connection waits in the connection queue until Node accepts it | **Right** | It is the accept queue, per listening socket, capped at about backlog + 1. |
| When does Node accept it? | | In the **poll phase**, once the listening fd is readable. If the thread is busy, the connection waits in the kernel. |
| After accepting, Node makes a system call to establish a new socket on port 8000 | **Partly wrong** | The kernel had already established the connection. `accept4()` just returns a **new fd** for the already-existing connection socket. That socket shares local port 8000 with the listener. |
| Incoming data sits in Recv-Q and Send-Q buffers | **Right** | Recv-Q = unread bytes that arrived. Send-Q = bytes sent but not yet acknowledged. (On a *listening* socket the same names mean accept-queue length and backlog.) |
| Node takes the data from Recv-Q and parses it | **Right** | `read()` copies it into Node's memory, and the C++ HTTP parser parses it. |
| Node adds `myRequestHandler` with the parsed request to the **microtask queue** | **Wrong** | The handler is **called directly and synchronously** from the parser callback. No queue is involved. Microtasks only enter if your code uses `await` or promises. |
| Does Node add the handler only when the microtask queue is empty? | **Wrong question** | Microtasks are drained after each callback. Reads happen in the poll phase, and nothing has to be empty first. Only a long callback or an endless microtask chain delays reading. |
| The response is placed into Send-Q after parsing | **Partly wrong** | The response is written when your handler calls `res.end()`, not after parsing. Node tries a synchronous `write()` into the kernel's send buffer. The kernel then transmits it. |

---

## 13. Notes on your sample code

1. **`console.log` with `toLocaleTimeString()` in the handler is the most expensive line.** It formats a locale string and does a `write()` system call to stdout on every request. At 20,000 requests per second that is a large share of your 36 µs budget. Remove it for any benchmark (your own Stage 0 rules say the same).
2. **`async` is not needed.** There is no `await`. It adds a Promise per request and turns a thrown error into an unhandled rejection.
3. **`writeHead` before `end` produces chunked encoding.** My test response came back with `Transfer-Encoding: chunked`, because `writeHead` serialises the headers immediately and Node cannot add a `Content-Length` afterwards. Setting `content-length` yourself, or passing headers to `res.end` flow without calling `writeHead` first, gives a smaller response with less work.
4. **`server.listen(8000)` without a host** binds the wildcard address (IPv6 `::` if available, which also accepts IPv4). That is why `ss` shows `*:8000`.
5. **Defaults that affect load tests:** backlog 511, `keepAliveTimeout` 5 s. A client that pauses longer than 5 seconds finds its connection closed.
