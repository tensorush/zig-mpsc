const std = @import("std");

const Self = @This();

const PollResult = enum {
    Empty,
    Retry,
    Item,
};

const Node = struct {
    next_opt: ?*Node = null,
};

head: *Node,
tail: *Node,
stub: Node,

pub fn init(self: *Self) void {
    @atomicStore(*Node, &self.head, &self.stub, .Monotonic);
    @atomicStore(*Node, &self.tail, &self.stub, .Monotonic);
    @atomicStore(?*Node, &self.stub.next_opt, null, .Monotonic);
}

pub fn push(self: *Self, node: *Node) void {
    pushOrdered(self, node, node);
}

pub fn pushOrdered(self: *Self, first: *Node, last: *Node) void {
    @atomicStore(?*Node, &last.next_opt, null, .Monotonic);
    const prev = @atomicRmw(*Node, &self.head, .Xchg, last, .AcqRel);
    @atomicStore(?*Node, &prev.next_opt, first, .Release);
}

pub fn pushUnordered(self: *Self, nodes: []*Node) void {
    if (nodes.len == 0) {
        return {};
    }

    var first = nodes[0];
    var last = nodes[nodes.len - 1];

    var i: usize = 0;
    while (i < nodes.len - 1) : (i += 1) {
        @atomicStore(?*Node, &nodes[i].next_opt, nodes[i + 1], .Monotonic);
    }

    pushOrdered(self, first, last);
}

pub fn isEmpty(self: *Self) bool {
    var tail = @atomicLoad(*Node, &self.tail, .Monotonic);
    const next_opt = @atomicLoad(?*Node, &tail.next_opt, .Acquire);
    const head = @atomicLoad(*Node, &self.head, .Acquire);
    return tail == &self.stub and next_opt == null and tail == head;
}

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

    push(self, &self.stub);

    next_opt = @atomicLoad(?*Node, &tail.next_opt, .Acquire);
    if (next_opt) |next| {
        @atomicStore(*Node, &self.tail, next, .Monotonic);
        node.* = tail;
        return .Item;
    }

    return .Retry;
}

pub fn pop(self: *Self) ?*Node {
    var result = PollResult.Retry;
    var node: *Node = undefined;

    while (result == .Retry) {
        result = poll(self, &node);
        if (result == .Empty) {
            return null;
        }
    }

    return node;
}

pub fn pushFrontByConsumer(self: *Self, node: *Node) void {
    var tail = @atomicLoad(*Node, &self.tail, .Monotonic);
    @atomicStore(?*Node, &node.next_opt, tail, .Monotonic);
    @atomicStore(*Node, &self.tail, node, .Monotonic);
}

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

test "push and pop" {
    var elements: [10]Element = undefined;
    var queue: Self = undefined;
    init(&queue);

    for (elements[0..], 0..) |*element, i| {
        element.id = i;
        queue.push(&element.node);
    }

    try std.testing.expectEqual(false, queue.isEmpty());

    var node_opt = queue.pop();
    while (node_opt) |node| : (node_opt = queue.pop()) {
        const element = @fieldParentPtr(Element, "node", node);
        try std.testing.expectEqual(&elements[element.id], element);
    }

    try std.testing.expectEqual(true, queue.isEmpty());
}
