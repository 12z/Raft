import sys
import time
import thriftpy
import random
import queue
import xmlrpc.client
import collections

from threading import Thread, Lock
from enum import Enum
# from thriftpy.rpc import make_server, make_client, client_context

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

thrift_service = thriftpy.load("messages.thrift", module_name="service_thrift")

total_number_of_nodes = 5
majority_threshold = total_number_of_nodes / 2


def append_entries(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
    reply = (term, True)
    return reply


def request_vote(term, candidate_id, last_log_index, last_log_term):
    reply = (term, True)
    return reply


response_list = ['pong', 'smth', 'string', 'idk']


def ping():
    reply = random.choice(response_list)
    print('rpc ', reply)
    return reply


class Request:
    def __init__(self, type_of, params):
        self.type = RequestType(type_of)
        self.params = params


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


class State(Enum):
    follower = 1
    candidate = 2
    leader = 3


class RequestType(Enum):
    append_entries = 1
    request_vote = 2


class TimerFlag:
    def __init__(self, flag, lock):
        self.flag = flag
        self.lock = lock

    def set_value(self, value):
        self.lock.acquire()
        self.flag = value
        self.lock.release()

    def get(self):
        self.lock.acquire()
        value = self.flag
        self.lock.release()
        return value


class VotedForMe:
    def __init__(self, value, lock):
        self.value = value
        self.lock = lock

    def increment(self):
        self.lock.acquire()
        self.value += 1
        self.lock.release()

    def reset(self, value=0):
        self.lock.acquire()
        self.value = value
        self.lock.release()

    def get(self):
        self.lock.acquire()
        value = self.value
        self.lock.release()
        return value


class NodeState:
    def __init__(self):
        self.lock = Lock()
        self.state = State.follower

    def set_state(self, state):
        self.lock.acquire()
        self.state = state
        self.lock.release()

    def get_state(self):
        self.lock.acquire()
        state = self.state
        self.lock.release()
        return state

class LeaderState:
    def __init__(self):
        servers = {}
        for i in range(1, total_number_of_nodes):
            servers[i] = (0, 0)


class Node:
    current_term = 0
    voted_for = 0
    votes_for_me = VotedForMe(0, Lock())
    log = []
    commit_index = 0
    last_applied = 0
    leader_state = LeaderState()

    queues = {}
    replies_queue = queue.Queue()
    client_queue = queue.Queue()
    # queue = queue.Queue()
    state_lock = Lock()  # acquire/release
    state = NodeState()
    port = 0

    multiplier = 10  # increase timeouts for debug purposes
    nodes = []

    # Timer flag False means event happened within interval timer should not act
    # True means that within interval no messages were received timer has to act
    # timer_flag = False  # initially assume that timer just was reset
    timer_flag = TimerFlag(False, Lock())

    # def ping(self, client):
    #     print(client.ping())

    def execute_request(self, receiver, request):
        if request.type == RequestType.append_entries:
            try:
                term, success = receiver.append_entries(request.params)
                return request.type, term, success
            except ConnectionRefusedError:
                pass
        if request.type == RequestType.request_vote:
            try:
                term, vote_granted = receiver.request_vote(request.params)
                return request.type, term, vote_granted
            except ConnectionRefusedError:
                pass
        return None

    def start_sender(self, port):
        print('starting sender to ', port)
        while True:
            while True:
                try:
                    s = xmlrpc.client.ServerProxy('http://localhost:' + str(port))
                    break
                except:
                    continue

            while True:
                try:
                    queue_entry = self.queues[port].get()
                    reply = self.execute_request(s, queue_entry)
                    if reply is not None:
                        self.replies_queue.put(reply)
                    # term, success = s.append_entries(1, 1, 1, 1, 1, 1)
                    # print('term ', term, ', success ', success)
                    # time.sleep(random.randint(1, 2))
                except queue.Empty:
                    continue
            continue

    def process_request_vote_reply(self, reply):
        pass

    def process_append_entries_reply(self, reply):
        pass

    def reply_processor(self):
        while True:
            try:
                reply = self.replies_queue.get()
            except queue.Empty:
                continue

            if reply.type == RequestType.request_vote:
                self.process_request_vote_reply(reply)
            if reply.type == RequestType.append_entries:
                self.process_append_entries_reply(reply)

    def leader_heartbeat(self):
        while True:
            if self.state.get_state() != State.leader:
                return

            if self.timer_flag.get():
                for node in self.nodes:
                    if self.port != node:
                        self.queues[node].put(Request(RequestType.append_entries,
                                              (self.current_term, self.port, 1, 1, [], self.commit_index)))
                        # Here we have mess with arguments for append entries which are unknown how to store

            self.timer_flag.set_value(True)
            time.sleep(0.030 * self.multiplier)

    def leader_loop(self):
        # Here we send messages to other nodes
        while True:
            try:
                record = self.client_queue.get()
            except queue.Empty:
                continue
            for node in self.nodes:
                if self.port != node:
                    self.queues[node].put(Request(record))  # todo change this to actual request
            self.timer_flag.set_value(False)

    def election_timer(self):
        self.timer_flag.set_value(False)
        interval = random.uniform(0.150 * self.multiplier, 0.300 * self.multiplier)  # from 150 to 300 milliseconds
        print('Election timeout is ', interval, ' secs')
        time.sleep(interval)
        self.timer_flag.set_value(True)

    def candidate_loop(self):
        # here we initiate election
        print('starting election, my port ', self.port)
        self.current_term += 1
        self.voted_for = self.port
        self.votes_for_me.reset(1)
        for node in self.nodes:
            if node != self.port:
                self.queues[node].put(Request(RequestType.request_vote,
                                      (self.current_term, self.port, self.last_applied, self.last_applied)))
                # Here we have mess with arguments for request vote which are unknown how to store
        Thread(target=self.election_timer(), args=()).start()
        while True:
            if self.votes_for_me.get() > majority_threshold:
                return State.leader
            if self.timer_flag.get():
                return State.candidate  # restart election
            if self.state == State.follower:
                return State.follower

    def start_receiver(self, port):
        server = SimpleXMLRPCServer(('localhost', port), requestHandler=RequestHandler)
        # listen messages to this node

        # the messages that this receiver serves
        server.register_function(append_entries)
        server.register_function(ping)

        print('serving on ', self.port)
        server.serve_forever()

    def follower_timer(self):
        print('started follower timer')
        while True:
            self.timer_flag.set_value(True)
            interval = random.uniform(0.150 * self.multiplier, 0.300 * self.multiplier)  # from 150 to 300 milliseconds
            time.sleep(interval)
            is_changed = False
            self.timer_flag.lock.acquire()
            if self.timer_flag.flag:
                is_changed = True
            self.timer_flag.lock.release()
            if is_changed:
                return State.candidate

    def state_handler(self):
        # we start at the follower state
        print('starting state handler')
        self.state.set_state(self.follower_timer())
        print('exited first follower loop')
        while True:  # will be switching between states here
            if self.state.get_state() == State.candidate:
                self.state.set_state(self.candidate_loop())
                continue
            if self.state.get_state() == State.follower:
                self.state.set_state(self.follower_timer())
                continue
            if self.state.get_state() == State.leader:
                self.state.set_state(self.leader_loop())
                continue

    def main(self, argv):
        self.port = int(argv[0])
        self.nodes = [9001, 9002, 9003, 9004, 9005]  # replace it with config file with ip and port
        for node in self.nodes:
            self.queues[node] = queue.Queue()
            if int(self.port) == node:
                Thread(target=self.start_receiver, args=(node,)).start()
            else:
                # thread for each other node to sen messages
                # s = xmlrpc.client.ServerProxy('http://localhost:' + str(node))
                Thread(target=self.start_sender, args=(node,)).start()

        Thread(target=self.state_handler, args=()).start()
        Thread(target=self.reply_processor, args=()).start()


if __name__ == '__main__':
    my_node = Node()
    Node.main(my_node, sys.argv[1:])
