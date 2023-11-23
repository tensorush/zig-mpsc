//! Intrusive wait-free MPSC queue implementation.

const std = @import("std");

const Self = @This();

/// Result status of queue polling.
pub const PollResult = enum {
    Empty,
    Retry,
    Item,
};

/// Intrusive queue node.
pub const Node = struct {
    next_opt: ?*Node,
};

/// Pointer to the start of the queue.
head: *Node,
/// Pointer to the end of the queue.
tail: *Node,
/// Stub node for null pointers.
stub: Node,

/// Initializes the MPSC queue.
pub fn init(self: *Self) void {
    @atomicStore(*Node, &self.head, &self.stub, .Monotonic);
    @atomicStore(*Node, &self.tail, &self.stub, .Monotonic);
    @atomicStore(?*Node, &self.stub.next_opt, null, .Monotonic);
}

/// Push a single node.
pub fn push(self: *Self, node: *Node) void {
    self.pushOrdered(node, node);
}

/// Push an ordered list of nodes in a single operation.
/// The nodes must all be appropriately linked from first to last.
pub fn pushOrdered(self: *Self, first: *Node, last: *Node) void {
    @atomicStore(?*Node, &last.next_opt, null, .Monotonic);
    const prev = @atomicRmw(*Node, &self.head, .Xchg, last, .AcqRel);
    @atomicStore(?*Node, &prev.next_opt, first, .Release);
}

/// Push a number of nodes at once.
/// The nodes will be appropriately linked together
/// before being inserted into the queue.
pub fn pushUnordered(self: *Self, nodes: []*Node) void {
    if (nodes.len == 0) {
        return {};
    }

    const first = nodes[0];
    const last = nodes[nodes.len - 1];

    var i: usize = 0;
    while (i < nodes.len - 1) : (i += 1) {
        @atomicStore(?*Node, &nodes[i].next_opt, nodes[i + 1], .Monotonic);
    }

    self.pushOrdered(first, last);
}

/// Checks if the queue is empty.
pub fn isEmpty(self: *Self) bool {
    var tail = @atomicLoad(*Node, &self.tail, .Monotonic);
    const next_opt = @atomicLoad(?*Node, &tail.next_opt, .Acquire);
    const head = @atomicLoad(*Node, &self.head, .Acquire);
    return tail == &self.stub and next_opt == null and tail == head;
}

/// Polls the queue for consuming the front node from the queue.
pub fn poll(self: *Self, node: **Node) PollResult {
    var head: *Node = undefined;
    var tail = @atomicLoad(*Node, &self.tail, .Monotonic);
    var next_opt = @atomicLoad(?*Node, &tail.next_opt, .Acquire);

    if (tail == &self.stub) {
        if (next_opt) |next| {
            @atomicStore(*Node, &self.tail, next, .Monotonic);
            tail = next;
            next_opt = @atomicLoad(?*Node, &tail.next_opt, .Acquire);
        } else {
            head = @atomicLoad(*Node, &self.head, .Acquire);
            return if (tail != head) .Retry else .Empty;
        }
    }

    if (next_opt) |next| {
        @atomicStore(*Node, &self.tail, next, .Monotonic);
        node.* = tail;
        return .Item;
    }

    head = @atomicLoad(*Node, &self.head, .Acquire);
    if (tail != head) {
        return .Retry;
    }

    self.push(&self.stub);

    next_opt = @atomicLoad(?*Node, &tail.next_opt, .Acquire);
    if (next_opt) |next| {
        @atomicStore(*Node, &self.tail, next, .Monotonic);
        node.* = tail;
        return .Item;
    }

    return .Retry;
}

/// Pops the front node from the queue.
pub fn pop(self: *Self) ?*Node {
    var result = PollResult.Retry;
    var node: *Node = undefined;

    while (result == .Retry) {
        result = self.poll(&node);
        if (result == .Empty) {
            return null;
        }
    }

    return node;
}

/// Push at the front of the queue.
/// Only the consumer is allowed to do that.
pub fn pushFrontByConsumer(self: *Self, node: *Node) void {
    const tail = @atomicLoad(*Node, &self.tail, .Monotonic);
    @atomicStore(?*Node, &node.next_opt, tail, .Monotonic);
    @atomicStore(*Node, &self.tail, node, .Monotonic);
}

/// Returns the last node of the queue.
pub fn getTail(self: *Self) ?*Node {
    var tail = @atomicLoad(*Node, &self.tail, .Monotonic);
    const next_opt = @atomicLoad(?*Node, &tail.next_opt, .Acquire);

    if (tail == &self.stub) {
        if (next_opt) |next| {
            @atomicStore(*Node, &self.tail, next, .Monotonic);
            tail = next;
        } else {
            return null;
        }
    }

    return tail;
}

/// Returns the next node.
pub fn getNext(self: *Self, prev: *Node) ?*Node {
    var next_opt = @atomicLoad(?*Node, &prev.next_opt, .Acquire);

    if (next_opt) |next| {
        if (next == &self.stub) {
            next_opt = @atomicLoad(?*Node, &next.next_opt, .Acquire);
        }
    }

    return next_opt;
}

const Element = struct {
    node: Node,
    id: usize,
};

test "ordered push, get, and pop" {
    var elements: [10]Element = undefined;
    var queue: Self = undefined;
    init(&queue);

    // Push
    for (elements[0..], 0..) |*element, i| {
        element.id = i;
        queue.push(&element.node);
    }

    try std.testing.expect(!queue.isEmpty());

    // Get
    var node_opt = queue.getTail();
    while (node_opt) |node| : (node_opt = queue.getNext(node)) {
        const element = @fieldParentPtr(Element, "node", node);
        try std.testing.expectEqual(&elements[element.id], element);
    }

    try std.testing.expect(!queue.isEmpty());

    // Pop
    node_opt = queue.pop();
    while (node_opt) |node| : (node_opt = queue.pop()) {
        const element = @fieldParentPtr(Element, "node", node);
        try std.testing.expectEqual(&elements[element.id], element);
    }

    try std.testing.expect(queue.isEmpty());
}

test "partial ordered push, get, and pop" {
    var elements: [10]Element = undefined;
    var prevs: [elements.len]*Node = undefined;
    var queue: Self = undefined;
    init(&queue);

    // Partial push start
    for (elements[0..], 0..) |*element, i| {
        element.id = i;
        if (i > elements.len / 2) {
            @atomicStore(?*Node, &element.node.next_opt, null, .Monotonic);
            prevs[i] = @atomicRmw(*Node, &queue.head, .Xchg, &element.node, .AcqRel);
        } else {
            queue.push(&element.node);
        }
    }

    // Get
    var node_opt = queue.getTail();
    while (node_opt) |node| : (node_opt = queue.getNext(node)) {
        const element = @fieldParentPtr(Element, "node", node);
        try std.testing.expectEqual(&elements[element.id], element);
    }

    try std.testing.expect(!queue.isEmpty());

    // Partial push end
    for (elements[(elements.len / 2) + 1 ..], (elements.len / 2) + 1..) |*element, i| {
        @atomicStore(?*Node, &prevs[i].next_opt, &element.node, .Release);
    }

    try std.testing.expect(!queue.isEmpty());

    // Get
    node_opt = queue.getTail();
    while (node_opt) |node| : (node_opt = queue.getNext(node)) {
        const element = @fieldParentPtr(Element, "node", node);
        try std.testing.expectEqual(&elements[element.id], element);
    }

    try std.testing.expect(!queue.isEmpty());

    // Pop
    node_opt = queue.pop();
    while (node_opt) |node| : (node_opt = queue.pop()) {
        const element = @fieldParentPtr(Element, "node", node);
        try std.testing.expectEqual(&elements[element.id], element);
    }

    try std.testing.expect(queue.isEmpty());
}

test "unordered push, get, and pop" {
    var elements: [1000]Element = undefined;
    var nodes: [64]*Node = undefined;
    var queue: Self = undefined;
    init(&queue);

    var prng = std.rand.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.os.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    const rng = prng.random();

    var batch_size: usize = undefined;
    var i: usize = 0;
    while (i < elements.len) : (i += batch_size) {
        batch_size = @max(1, rng.uintLessThan(usize, @min(nodes.len, elements.len - i)));

        var j: usize = 0;
        while (j < batch_size) : (j += 1) {
            elements[i + j].id = i + j;
            nodes[j] = &elements[i + j].node;
        }
        queue.pushUnordered(nodes[0..batch_size]);
        try std.testing.expect(!queue.isEmpty());
    }

    try std.testing.expect(!queue.isEmpty());

    // Get
    var node_opt = queue.getTail();
    while (node_opt) |node| : (node_opt = queue.getNext(node)) {
        const element = @fieldParentPtr(Element, "node", node);
        try std.testing.expectEqual(&elements[element.id], element);
    }

    try std.testing.expect(!queue.isEmpty());

    // Pop
    node_opt = queue.pop();
    while (node_opt) |node| : (node_opt = queue.pop()) {
        const element = @fieldParentPtr(Element, "node", node);
        try std.testing.expectEqual(&elements[element.id], element);
    }

    try std.testing.expect(queue.isEmpty());
}

test "partial push and poll" {
    var elements: [3]Element = undefined;
    var prevs: [elements.len]*Node = undefined;
    var queue: Self = undefined;
    var node: *Node = undefined;
    init(&queue);

    for (elements[0..], 0..) |*element, i| {
        element.id = i;
    }

    try std.testing.expectEqual(queue.poll(&node), .Empty);
    try std.testing.expect(queue.isEmpty());

    queue.push(&elements[0].node);
    try std.testing.expect(!queue.isEmpty());
    try std.testing.expectEqual(queue.poll(&node), .Item);
    try std.testing.expectEqual(queue.poll(&node), .Empty);

    queue.push(&elements[0].node);
    queue.push(&elements[1].node);
    try std.testing.expect(!queue.isEmpty());
    try std.testing.expectEqual(queue.poll(&node), .Item);
    try std.testing.expect(!queue.isEmpty());
    try std.testing.expectEqual(queue.poll(&node), .Item);
    try std.testing.expectEqual(queue.poll(&node), .Empty);

    // Partial push
    @atomicStore(?*Node, &elements[0].node.next_opt, null, .Monotonic);
    prevs[0] = @atomicRmw(*Node, &queue.head, .Xchg, &elements[0].node, .AcqRel);
    try std.testing.expectEqual(queue.poll(&node), .Retry);
    try std.testing.expectEqual(queue.poll(&node), .Retry);

    @atomicStore(?*Node, &prevs[0].next_opt, &elements[0].node, .Release);
    try std.testing.expectEqual(queue.poll(&node), .Item);
    try std.testing.expectEqual(queue.poll(&node), .Empty);

    // Full and partial push
    queue.push(&elements[0].node);
    queue.push(&elements[1].node);
    @atomicStore(?*Node, &elements[2].node.next_opt, null, .Monotonic);
    prevs[2] = @atomicRmw(*Node, &queue.head, .Xchg, &elements[2].node, .AcqRel);
    try std.testing.expectEqual(queue.poll(&node), .Item);
    try std.testing.expectEqual(queue.poll(&node), .Retry);
    try std.testing.expectEqual(queue.poll(&node), .Retry);

    @atomicStore(?*Node, &prevs[2].next_opt, &elements[2].node, .Release);
    try std.testing.expectEqual(queue.poll(&node), .Item);
    try std.testing.expectEqual(queue.poll(&node), .Item);
    try std.testing.expectEqual(queue.poll(&node), .Empty);

    // Partial push and poll start
    queue.push(&elements[0].node);

    var tail = @atomicLoad(*Node, &queue.tail, .Monotonic);
    var next_opt = @atomicLoad(?*Node, &tail.next_opt, .Acquire);
    var head: *Node = undefined;
    var is_done = false;

    if (tail == &queue.stub) {
        if (next_opt) |next| {
            @atomicStore(*Node, &queue.tail, next, .Monotonic);
            tail = next;
            next_opt = @atomicLoad(?*Node, &tail.next_opt, .Acquire);
        } else {
            head = @atomicLoad(*Node, &queue.head, .Acquire);
            if (tail != head) {
                is_done = true; // .Retry
            } else {
                is_done = true; // .Empty
            }
        }
    }

    if (next_opt) |next| {
        @atomicStore(*Node, &queue.tail, next, .Monotonic);
        is_done = true; // .Item
        node = tail;
    }

    head = @atomicLoad(*Node, &queue.head, .Acquire);
    if (tail != head) {
        is_done = true; // .Retry
    }

    try std.testing.expect(!is_done);

    @atomicStore(?*Node, &elements[1].node.next_opt, null, .Monotonic);
    prevs[1] = @atomicRmw(*Node, &queue.head, .Xchg, &elements[1].node, .AcqRel);

    // Partial push and poll end
    queue.push(&queue.stub);

    var poll_result = PollResult.Retry;

    next_opt = @atomicLoad(?*Node, &tail.next_opt, .Acquire);
    if (next_opt) |next| {
        @atomicStore(*Node, &queue.tail, next, .Monotonic);
        poll_result = .Item;
        node = tail;
    }

    try std.testing.expectEqual(poll_result, .Retry);

    @atomicStore(?*Node, &prevs[1].next_opt, &elements[1].node, .Release);
    try std.testing.expectEqual(queue.poll(&node), .Item);
    try std.testing.expectEqual(&elements[0].node, node);
    try std.testing.expectEqual(queue.poll(&node), .Item);
    try std.testing.expectEqual(&elements[1].node, node);
    try std.testing.expectEqual(queue.poll(&node), .Empty);
}

test "pushFrontByConsumer, get, and pop" {
    var elements: [10]Element = undefined;
    var queue: Self = undefined;
    init(&queue);

    // Push front by consumer
    try std.testing.expectEqual(queue.pop(), null);
    queue.pushFrontByConsumer(&elements[0].node);
    try std.testing.expect(!queue.isEmpty());
    var node_opt = queue.pop();
    try std.testing.expectEqual(node_opt, &elements[0].node);
    try std.testing.expectEqual(queue.pop(), null);
    try std.testing.expect(queue.isEmpty());

    queue.pushFrontByConsumer(&elements[0].node);
    queue.pushFrontByConsumer(&elements[1].node);
    try std.testing.expect(!queue.isEmpty());
    try std.testing.expectEqual(queue.pop(), &elements[1].node);
    try std.testing.expectEqual(queue.pop(), &elements[0].node);
    try std.testing.expectEqual(queue.pop(), null);

    queue.pushFrontByConsumer(&elements[1].node);
    queue.pushFrontByConsumer(&elements[0].node);
    queue.push(&elements[2].node);
    try std.testing.expectEqual(queue.pop(), &elements[0].node);
    try std.testing.expectEqual(queue.pop(), &elements[1].node);
    try std.testing.expectEqual(queue.pop(), &elements[2].node);
    try std.testing.expectEqual(queue.pop(), null);

    // Push
    for (elements[0..], 0..) |*element, i| {
        element.id = i;
        queue.push(&element.node);
    }

    // Pop and push front by consumer
    node_opt = queue.pop();
    queue.pushFrontByConsumer(node_opt.?);
    try std.testing.expectEqual(node_opt, queue.pop());
    queue.pushFrontByConsumer(node_opt.?);

    try std.testing.expect(!queue.isEmpty());

    // Get
    node_opt = queue.getTail();
    while (node_opt) |node| : (node_opt = queue.getNext(node)) {
        const element = @fieldParentPtr(Element, "node", node);
        try std.testing.expectEqual(&elements[element.id], element);
    }

    try std.testing.expect(!queue.isEmpty());

    // Pop
    node_opt = queue.pop();
    while (node_opt) |node| : (node_opt = queue.pop()) {
        const element = @fieldParentPtr(Element, "node", node);
        try std.testing.expectEqual(&elements[element.id], element);
    }

    try std.testing.expect(queue.isEmpty());
}
