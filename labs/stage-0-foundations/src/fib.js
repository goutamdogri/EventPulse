// Shared by /cpu (main thread) and worker.js, so both compute the same thing.
export const fib = (n) => (n < 2 ? n : fib(n - 1) + fib(n - 2));
