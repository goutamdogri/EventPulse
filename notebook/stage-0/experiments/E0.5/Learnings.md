# Learnings for experiment E0.5

This experiment is about understanding what happens when your server runs out of file descriptors.

A file descriptor (FD) is a small integer that Linux gives a process to identify an open resource.

Examples:

- An open file.
- A TCP connection.
- A listening socket.
- A pipe.

Imagine your server has an FD limit of 256.

It needs some descriptors for its internal resources. Every new client connection also consumes a descriptor.
Eventually, it cannot accept another connection.

Linux returns:
`EMFILE`: Too many open files
This is what we want to demonstrate.

Our hypothesis:
If we restrict the server to 256 file descriptors and continuously open connections, the server will eventually fail to accept new connections.
