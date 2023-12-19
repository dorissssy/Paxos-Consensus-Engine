package paxos

import (
	"coms4113/hw5/pkg/base"
)
// import "fmt"

const (
	Propose = "propose"
	Accept  = "accept"
	Decide  = "decide"
)

type Proposer struct {
	N             int
	Phase         string
	N_a_max       int
	V             interface{}
	SuccessCount  int
	ResponseCount int
	// To indicate if response from peer is received, should be initialized as []bool of len(server.peers)
	Responses []bool
	// Use this field to check if a message is latest.
	SessionId int

	// in case node will propose again - restore initial value
	InitialValue interface{}
}

type ServerAttribute struct {
	peers []base.Address
	me    int

	// Paxos parameter
	n_p int
	n_a int
	v_a interface{}

	// final result
	agreedValue interface{}

	// Propose parameter
	proposer Proposer

	// retry
	timeout *TimeoutTimer
}

type Server struct {
	base.CoreNode
	ServerAttribute
}

func NewServer(peers []base.Address, me int, proposedValue interface{}) *Server {
	response := make([]bool, len(peers))
	return &Server{
		CoreNode: base.CoreNode{},
		ServerAttribute: ServerAttribute{
			peers: peers,
			me:    me,
			proposer: Proposer{
				InitialValue: proposedValue,
				Responses:    response,
			},
			timeout: &TimeoutTimer{},
		},
	}
}

func (server *Server) MessageHandler(message base.Message) []base.Node {
	//TODO: implement it
	newNodes := make([]base.Node, 0)
	proposeRequest, ok := message.(*ProposeRequest)
	if ok {
		node := server.copy()
		responses := make([]base.Message, 0)
		if proposeRequest.N > node.n_p {
			node.n_p = proposeRequest.N
			response := &ProposeResponse{}
			response.CoreMessage= base.MakeCoreMessage(message.To(), message.From())
			response.Ok = true
			response.N_p = proposeRequest.N
			response.N_a = node.n_a
			response.V_a = node.v_a
			response.SessionId = proposeRequest.SessionId
			responses = append(responses, response)
		} else {
			response := &ProposeResponse{}
			response.CoreMessage= base.MakeCoreMessage(message.To(), message.From())
			response.Ok = false
			response.N_p = node.n_p
			response.SessionId = proposeRequest.SessionId
			responses = append(responses, response)
		}
		node.SetResponse(responses)
		newNodes = append(newNodes, node)
	}

	proposeResponse, ok := message.(*ProposeResponse)
	if ok {
		num := server.getNumber(proposeResponse.From())
		node:= server.copy()
		if server.proposer.Responses[num] == true || num == -1 || proposeResponse.SessionId != server.proposer.SessionId {
			newNodes = append(newNodes, node)
		} else {
			// immediate response from an acceptor and make updates
			node.proposer.Responses[num] = true
			node.proposer.ResponseCount += 1
			if proposeResponse.Ok {

				node.proposer.SuccessCount += 1
				if proposeResponse.N_a > node.proposer.N_a_max {
					node.proposer.N_a_max = proposeResponse.N_a
					node.proposer.V = proposeResponse.V_a
				}
				newNodes = append(newNodes, node)
				if node.proposer.SuccessCount > len(server.peers)/2 {
					node := server.copy()
					// reset and transition to next phase

					if proposeResponse.N_a > node.proposer.N_a_max {
						node.proposer.N_a_max = proposeResponse.N_a
						node.proposer.V = proposeResponse.V_a
					}
					node.proposer.Phase = Accept
					node.proposer.SuccessCount = 0
					node.proposer.ResponseCount = 0
					node.proposer.Responses = make([]bool, len(node.peers))
					responses := make([]base.Message, 0)
					for _, peer := range server.peers {
						r := &AcceptRequest{}
						r.CoreMessage = base.MakeCoreMessage(server.peers[server.ServerAttribute.me], peer)
						r.N = proposeResponse.N_p
						r.V = node.proposer.V
						r.SessionId = proposeResponse.SessionId
						responses = append(responses, r)
					}
					node.SetResponse(responses)
					newNodes = append(newNodes, node)
				}
				
			} else {
				newNodes = append(newNodes, node)
			}

		}
	}

	acceptRequest, ok := message.(*AcceptRequest)
	if ok {
		node := server.copy()
		response := &AcceptResponse{}
		responses := make([]base.Message, 0)
		if acceptRequest.N >= node.n_p {
			node.n_p = acceptRequest.N
			node.n_a = acceptRequest.N
			node.v_a = acceptRequest.V
			response.CoreMessage =base.MakeCoreMessage(acceptRequest.To(),acceptRequest.From())
			response.Ok = true
			response.N_p = acceptRequest.N
			response.SessionId = acceptRequest.SessionId

		} else {
			response.CoreMessage =base.MakeCoreMessage(acceptRequest.To(),acceptRequest.From())
			response.Ok = false
			response.N_p = node.n_p
			response.SessionId = acceptRequest.SessionId
		}
		responses = append(responses, response)
		node.SetResponse(responses)
		newNodes = append(newNodes, node)
	}

	accResp, ok := message.(*AcceptResponse)
	if ok {
		num := server.getNumber(accResp.From())
		node := server.copy()
		if server.proposer.Responses[num] == true || num == -1 || accResp.SessionId != server.proposer.SessionId {
			newNodes = append(newNodes, node)
		} else {
			node := server.copy()
			node.proposer.Responses[num] = true
			node.proposer.ResponseCount += 1
			if accResp.Ok {
				node.proposer.SuccessCount += 1

				newNodes = append(newNodes, node)
				if node.proposer.SuccessCount >= len(server.peers)/2 {
					node := server.copy()
					responses := make([]base.Message, 0)
					for _, peer := range server.peers {
						r := &DecideRequest{}
						r.CoreMessage = base.MakeCoreMessage(server.peers[server.ServerAttribute.me], peer)
						r.V = node.proposer.V
						r.SessionId = accResp.SessionId
						responses = append(responses, r)
					}
					node.proposer.Phase = Decide
					node.proposer.SuccessCount = 0
					node.proposer.ResponseCount = 0
					node.proposer.Responses = make([]bool, len(node.peers))
					node.agreedValue = node.proposer.V
					node.SetResponse(responses)
					newNodes = append(newNodes, node)
				}
			} else {
				newNodes = append(newNodes, node)
			}
		}
	}

	decideRequest, ok := message.(*DecideRequest)
	if ok {
		node:= server.copy()
		node.proposer.Phase = Decide
		node.agreedValue = decideRequest.V
		newNodes = append(newNodes, node)
	}

	return newNodes
}

func (server *Server) getNumber(address base.Address) int {
	for i, peer := range server.peers {
		if peer == address {
			return i
		}
	}
	return -1
}

// To start a new round of Paxos.
func (server *Server) StartPropose() {
	//TODO: implement it
	N := server.proposer.N + 1
	server.proposer.N = N
	server.proposer.Phase = Propose
	server.proposer.V = server.proposer.InitialValue
	server.proposer.SuccessCount = 0
	server.proposer.ResponseCount = 0
	server.proposer.Responses = make([]bool, len(server.peers))
	server.proposer.SessionId = 1
	responses := make([]base.Message, 0)
	for _, peer := range server.peers {
		resp := &ProposeRequest{}
		resp.CoreMessage= base.MakeCoreMessage(server.peers[server.ServerAttribute.me], peer)
		resp.N = N
		resp.SessionId = 1
		responses = append(responses, resp)
	}
	server.SetResponse(responses)


	
}

// Returns a deep copy of server node
func (server *Server) copy() *Server {
	response := make([]bool, len(server.peers))
	for i, flag := range server.proposer.Responses {
		response[i] = flag
	}

	var copyServer Server
	copyServer.me = server.me
	// shallow copy is enough, assuming it won't change
	copyServer.peers = server.peers
	copyServer.n_a = server.n_a
	copyServer.n_p = server.n_p
	copyServer.v_a = server.v_a
	copyServer.agreedValue = server.agreedValue
	copyServer.proposer = Proposer{
		N:             server.proposer.N,
		Phase:         server.proposer.Phase,
		N_a_max:       server.proposer.N_a_max,
		V:             server.proposer.V,
		SuccessCount:  server.proposer.SuccessCount,
		ResponseCount: server.proposer.ResponseCount,
		Responses:     response,
		InitialValue:  server.proposer.InitialValue,
		SessionId:     server.proposer.SessionId,
	}

	// doesn't matter, timeout timer is state-less
	copyServer.timeout = server.timeout

	return &copyServer
}

func (server *Server) NextTimer() base.Timer {
	return server.timeout
}

// A TimeoutTimer tick simulates the situation where a proposal procedure times out.
// It will close the current Paxos round and start a new one if no consensus reached so far,
// i.e. the server after timer tick will reset and restart from the first phase if Paxos not decided.
// The timer will not be activated if an agreed value is set.
func (server *Server) TriggerTimer() []base.Node {
	if server.timeout == nil {
		return nil
	}

	subNode := server.copy()
	subNode.StartPropose()

	return []base.Node{subNode}
}

func (server *Server) Attribute() interface{} {
	return server.ServerAttribute
}

func (server *Server) Copy() base.Node {
	return server.copy()
}

func (server *Server) Hash() uint64 {
	return base.Hash("paxos", server.ServerAttribute)
}

func (server *Server) Equals(other base.Node) bool {
	otherServer, ok := other.(*Server)

	if !ok || server.me != otherServer.me ||
		server.n_p != otherServer.n_p || server.n_a != otherServer.n_a || server.v_a != otherServer.v_a ||
		(server.timeout == nil) != (otherServer.timeout == nil) {
		return false
	}

	if server.proposer.N != otherServer.proposer.N || server.proposer.V != otherServer.proposer.V ||
		server.proposer.N_a_max != otherServer.proposer.N_a_max || server.proposer.Phase != otherServer.proposer.Phase ||
		server.proposer.InitialValue != otherServer.proposer.InitialValue ||
		server.proposer.SuccessCount != otherServer.proposer.SuccessCount ||
		server.proposer.ResponseCount != otherServer.proposer.ResponseCount {
		return false
	}

	for i, response := range server.proposer.Responses {
		if response != otherServer.proposer.Responses[i] {
			return false
		}
	}

	return true
}

func (server *Server) Address() base.Address {
	return server.peers[server.me]
}