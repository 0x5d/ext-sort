# ext-sort

An [external sorting](https://en.wikipedia.org/wiki/External_sorting) implementation written in Rust.

This is a WIP, just a small project to learn more Rust.


## Strategy

At a high level, the algorithm is comprised of a split/sort phase, followed by a k-way merge.

The sorting algorithm assumes that the source file is [page](https://en.wikipedia.org/wiki/Page_(computer_memory))-aligned (i.e. file size % 4096B == 0).

The sorting unit is one page (4096B), and pages are compared lexicographically.

### Split/sort phase

The source file is read concurrently at different offsets (configured by the `--int-file-size` flag). Each offset range ([0, K), [K, K\*2), ..., [K\*N-1, K\*N)) will determine an _intermediate_ file's content, which is unstable-sorted and then written to storage (the location is configured by `--int-file-dir`).

### K-way merge phase

Each intermediate file is guaranteed to be sorted, but now a global order (among all intermediate files) must be determined. To do so, a [binary heap](https://doc.rust-lang.org/std/collections/struct.BinaryHeap.html) is used.

The heap is pre-populated by reading one page (handled as a `[u8; 4096]`) from each intermediate file, which is `push`ed to the heap.

Then, while there are still pages in the heap, a page is `pop`ped, which is guaranteed to be the lesser element (by lexicographical value). The page is then appended to the output file, and a new page is read from the same file, "replacing" the popped page. If a page can't be read, it means the intermediate file's contents have been read in its entirety.

When the heap is empty, all the intermediate files' contents have been read and sorted through the heap, and the algorithm is done.

## Backlog

- Use Buf{Writer,Reader}
- Tests
- Make the block size configurable