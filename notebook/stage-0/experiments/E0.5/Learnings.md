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

## Explaination of the fd analysis command

```bash
ls -l /proc/$PID/fd | awk '{print $NF}' | cut -d: -f1 | sort | uniq -c
```

1. `ls -l /proc/$PID/fd`: Displays the symlink targets present at `/proc/$PID/fd`
2. `awk '{print $NF}'`: processes input line by line and prints that last field.
3. `cut -d: -f1`: `-d:` specifies `:` as the delimiter. `-f1` extracts the first field.
4. `sort`: Sorts the output alphabetically.
5. `uniq -c`: Counts consecutive duplicate lines.

```bash
sudo strace -f -tt -e trace=accept4,close,openat -p $PID -o /tmp/e05.strace
```

This command attaches strace to an already-running process and records selected system calls made by that process and its child processes.

1. `strace`: A Linux debugging utility that traces system calls and signals made by a process. For example, when an application opens a file, it eventually makes a system call such as: `openat(...)`
2. `-f` — Follow child processes. Follows processes created by the traced process using operations such as fork(), vfork() and clone().
3. `-tt` — Print timestamps. Adds timestamps to every traced system call.
4. `-e trace=accept4,close,openat` — Filter system calls. It tells strace to capture only these three system calls.
   - `accept4()`: Accepts an incoming connection on a listening socket.
   - `close(fd)`: Closes an open file descriptor.
   - `openat(dirfd, pathname, flags, mode)`: Opens or creates a file relative to a directory file descriptor.
5. `-p $PID` — Attach to an existing process.
6. `-o /tmp/e05.strace` — Save output. Writes trace output into this file instead of printing it to the terminal.
