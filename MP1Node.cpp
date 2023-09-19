/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * Definition of MP1Node class functions. (Revised 2020)
 *
 * Starter code template
 **********************************/
#include "MP1Node.h"
/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Params *params, EmulNet *emul, Log *log, Address *address) {
 for( int i = 0; i < 6; i++ ) {
 NULLADDR[i] = 0;
 }
 this->memberNode = new Member;
 this->shouldDeleteMember = true;
 memberNode->inited = false;
 this->emulNet = emul;
 this->log = log;
 this->par = params;
 this->memberNode->addr = *address;

 this->broadcast = 0;
 this->nodesToDelete = new std::map<int, int>;
}
/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {
 if (shouldDeleteMember) {
 delete this->memberNode;
 }
}
/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the
queue
 * This function is called by a node to receive messages currently
waiting for it
 */
int MP1Node::recvLoop() {
 if ( memberNode->bFailed ) {
 return false;
 }
 else {
 return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1,
&(memberNode->mp1q));
 }
}
/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
 Queue q;
 return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * All initializations routines for a member.
 * Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
 // join address is the address of the introducer
 Address joinaddr;
 joinaddr = getJoinAddress();
 Address *my_addr = &this->memberNode->addr;
 // Self booting routines
 if( initThisNode(my_addr) == -1 ) {
#ifdef DEBUGLOG
 log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
 exit(1);
 }
 if( !introduceSelfToGroup(&joinaddr) ) {
 finishUpThisNode();
#ifdef DEBUGLOG
 log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
 exit(1);
 }
 return;
}
/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *my_addr) {
 /*
 * This function is partially implemented and may require changes
 */
 int id = *(int*)(&memberNode->addr.addr);
 int port = *(short*)(&memberNode->addr.addr[4]);

 memberNode->bFailed = false;
 memberNode->inited = true;
 memberNode->inGroup = false;
 // node is up!
 memberNode->nnb = 0;
 memberNode->heartbeat = 0;
 memberNode->pingCounter = TFAIL;
 memberNode->timeOutCounter = -1;
 initMemberListTable(memberNode);

 memcpy(&id, &(memberNode->addr.addr[0]), sizeof(int));
 memcpy(&port, &(memberNode->addr.addr[4]), sizeof(short));
 MemberListEntry *newEntry = new MemberListEntry(id, port, 0, par-
>getcurrtime());
 memberNode->memberList.push_back(*newEntry);
 return 0;
}
/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
 MessageHdr *msg;
#ifdef DEBUGLOG
 static char s[1024];
#endif
 if ( 0 == strcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr)))
{
 // I am the group booter (first process to join the group). Boot up the
group
#ifdef DEBUGLOG
 log->LOG(&memberNode->addr, "Starting up group...");
#endif
 memberNode->inGroup = true;
 }
 else {
 size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long)
+ 1;
 msg = (MessageHdr *) malloc(msgsize * sizeof(char));
 // create JOINREQ message: format of data is {struct Address myaddr}
 msg->msgType = JOINREQ;
 memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode-
>addr.addr));
 memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode-
>heartbeat, sizeof(long));
#ifdef DEBUGLOG
 sprintf(s, "Trying to join...");
 log->LOG(&memberNode->addr, s);
#endif
 // send JOINREQ message to introducer member
 emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
 free(msg);
 }
 return 1;
}
/**
* FUNCTION NAME: finishUpThisNode
*
* DESCRIPTION: Wind up this node and clean up state
*/
int MP1Node::finishUpThisNode(){
 /*
 * Your code goes here
 */
 emulNet->ENcleanup();
 return 0;
}
/**
* FUNCTION NAME: nodeLoop
*
* DESCRIPTION: Executed periodically at each member
* Check your messages in queue and perform membership protocol
duties
*/
void MP1Node::nodeLoop() {
 if (memberNode->bFailed) {
 return;
 }
 // Check my messages
 checkMessages();
 // Wait until you're in the group...
 if( !memberNode->inGroup ) {
 return;
 }
 // ...then jump in and share your responsibilites!
 nodeLoopOps();
 return;
}
/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
 void *ptr;
 int size;
 // Pop waiting messages from memberNode's mp1q
 while ( !memberNode->mp1q.empty() ) {
 ptr = memberNode->mp1q.front().elt;
 size = memberNode->mp1q.front().size;
 memberNode->mp1q.pop();
 recvCallBack((void *)memberNode, (char *)ptr, size);
 }
 return;
}
/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
 /*
 * Your code goes here
 */
 // Determine what type of message we are receiving and forward it to helper
methods
 MsgTypes msgType = ((MessageHdr *)data)->msgType;
 // Forward to respective helper methods
 switch (msgType) {
 case JOINREP:
 MBLISTmsg(env, data, size);
 memberNode->inGroup = true;
 break;
 case JOINREQ:
 JOINREQmsg(env, data, size);
 break;
 case MBLIST:
 MBLISTmsg(env, data, size);
 break;
 default:
 break;
 }
 return false;
}
/**
 * FUNCTION NAME: JOINREQmsg
 *
 * DESCRIPTION: Message handler for JOINREQ
 */
void MP1Node::JOINREQmsg(void *env, char *data, int size) {
 char addr[6];
 MessageHdr *m = (MessageHdr *)data;
 memcpy(&addr, (char *)(m)+4, 6);

 int id = 0;
 short port;
 memcpy(&id, &addr[0], sizeof(int));
 memcpy(&port, &addr[4], sizeof(short));
 MemberListEntry *newEntry = new MemberListEntry(id, port, 0, par-
>getcurrtime());
 memberNode->memberList.push_back(*newEntry);

 Address address = Address(to_string(id) + ":" + to_string(port));

 log->logNodeAdd(&memberNode->addr, &address);

 int entry_num = memberNode->memberList.size();
 int currentOffset = 0;
 size_t JOINREPMsgSize = sizeof(MessageHdr) + (sizeof(addr) + sizeof(long)) *
entry_num;
 MessageHdr *JOINREPMsg;
 JOINREPMsg = (MessageHdr *) malloc(JOINREPMsgSize * sizeof(char));
 JOINREPMsg->msgType = JOINREP;
 currentOffset += sizeof(int);
 for (MemberListEntry memberListEntry: memberNode->memberList) {
 address = convertToAddr(to_string(memberListEntry.getid()),
to_string(memberListEntry.getport()));
 memcpy((char *)(JOINREPMsg) + currentOffset, &address.addr,
sizeof(address.addr));
 currentOffset += sizeof(address.addr);
 memcpy((char *)(JOINREPMsg) + currentOffset, &memberListEntry.heartbeat,
sizeof(long));
 currentOffset += sizeof(long);
 }

 address = convertToAddr(to_string(id), to_string(port));
 emulNet->ENsend(&memberNode->addr, &address, (char *)JOINREPMsg,
currentOffset);
}
/**
 * FUNCTION NAME: MEMBERLISTmsg
 *
 * DESCRIPTION: Message handler for MEMBERLIST
 */
void MP1Node::MBLISTmsg(void *env, char *data, int size) {
 int entry = (size - 4)/ (14);
 int offset = 4;
 char addr[6];
 int id = 0;
 short port;
 long heartbeat = 0;
 MessageHdr *msg = (MessageHdr *)data;
 memcpy(&addr, (char *)(msg)+offset, 6);
 memcpy(&id, &addr[0], sizeof(int));
 if(this->shouldDeleteMember){
 std::map<int, int>::iterator itr = this->nodesToDelete->begin();

 while(itr != this->nodesToDelete->end()) {
 if (itr->first == id) {
 this->nodesToDelete->erase(itr);
 break;
 }
 itr++;
 }
 }
 for (int i = 0; i < entry; i++) {
 memcpy(&addr, (char *)(msg)+offset, 6);
 offset += 6;
 memcpy(&id, &addr[0], sizeof(int));
 memcpy(&port, &addr[4], sizeof(short));
 memcpy(&heartbeat, (char *)(msg)+offset, 8);
 offset += 8;
 updateEntry(memberNode, id, port, heartbeat);
 }
}
/**
 * FUNCTION NAME: updateEntry
 *
 * DESCRIPTION: Helper Method to update entries
 */
void MP1Node::updateEntry(Member *memberNode, int id, short port, long heartbeat) {
 bool found = false;
 for (MemberListEntry memberListEntry: memberNode->memberList) {
 if (memberListEntry.getid() == id && memberListEntry.getport() == port) {
 found = true;
 if (memberListEntry.getheartbeat() < heartbeat) {
 memberListEntry.setheartbeat(heartbeat);
 memberListEntry.settimestamp(par->getcurrtime());
 }
 break;
 }
 }
 if (!found) {
 MemberListEntry *newEntry = new MemberListEntry(id, port, heartbeat, par-
>getcurrtime());
 memberNode->memberList.push_back(*newEntry);
 Address address = convertToAddr(to_string(id),to_string(port));

 log->logNodeAdd(&memberNode->addr, &address);
 }
}
/**
 * FUNCTION NAME: deleteNodes
 *
 * DESCRIPTION: Helper method to delete nodes
 */
void MP1Node::deleteNodes() {
 std::map<int, int>::iterator itr = this->nodesToDelete->begin();

 while(itr != this->nodesToDelete->end()) {
 itr->second = itr->second + 1;
 if (itr->second >= TREMOVE) {
 Address address = convertToAddr(to_string(itr->first),to_string(0));
 log->logNodeRemove(&memberNode->addr, &address);
 bool found = false;
 memberNode->myPos = memberNode->memberList.begin();
 while (memberNode->myPos != memberNode->memberList.end()) {
 if ((*memberNode->myPos).getid() == itr->first) {
 found = true;
 memberNode->myPos = memberNode->memberList.erase(memberNode-
>myPos);
 break;
 }
 memberNode->myPos++;
 }
 itr = this->nodesToDelete->erase(itr);
 }
 else {
 itr++;
 }
 }
}
/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then
delete
 * the nodes
 * Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
 /*
 * Your code goes here
 */
 memberNode->heartbeat++;
 memberNode->memberList[0].setheartbeat(memberNode->heartbeat);
 memberNode->memberList[0].settimestamp(par->getcurrtime());
 memberNode->myPos = memberNode->memberList.begin();
 while (memberNode->myPos != memberNode->memberList.end()) {
 if(memberNode->memberList[0].gettimestamp() - memberNode->myPos-
>gettimestamp() > TFAIL){
 int id = memberNode->myPos->getid();
 if(this->shouldDeleteMember){
 this->nodesToDelete->insert(std::make_pair(id, 0));
 }
 }
 memberNode->myPos++;
 }
 this->deleteNodes();
 int entry_num = memberNode->memberList.size();
 int currentOffset = 0;
 size_t memberListMsgSize = sizeof(MessageHdr) + (6 + sizeof(long)) * entry_num;
 MessageHdr *memberListMsg;
 memberListMsg = (MessageHdr *) malloc(memberListMsgSize * sizeof(char));
 memberListMsg->msgType = MBLIST;
 currentOffset += sizeof(int);
 for (MemberListEntry memberListEntry: memberNode->memberList) {
 if (this->nodesToDelete->count(memberListEntry.getid()) != 0) {
 continue;
 }
 Address address = convertToAddr(to_string(memberListEntry.getid()),
to_string(memberListEntry.getport()));
 memcpy((char *)(memberListMsg) + currentOffset, &address.addr,
sizeof(address.addr));
 currentOffset += sizeof(address.addr);
 memcpy((char *)(memberListMsg) + currentOffset, &memberListEntry.heartbeat,
sizeof(long));
 currentOffset += sizeof(long);
 }
 broadcast++;
 if (broadcast <= BROADCAST_INTERVAL) {
 for (MemberListEntry memberListEntry: memberNode->memberList) {
 Address address = convertToAddr(to_string(memberListEntry.getid()),
to_string(memberListEntry.getport()));
 emulNet->ENsend(&memberNode->addr, &address, (char *)memberListMsg,
currentOffset);
 }
 broadcast = 0;
 }
 else {
 int member_num = memberNode->memberList.size();
 int index = rand() % member_num;
 Address address = convertToAddr(to_string(memberNode-
>memberList.at(index).getid()), to_string(memberNode-
>memberList.at(index).getport()));
 emulNet->ENsend(&memberNode->addr, &address, (char *)memberListMsg,
currentOffset);
 }
 free(memberListMsg);
 return;
}
/**
 * FUNCTION NAME: convertToAddr
 *
 * DESCRIPTION: Converts format to Addr
 */
Address MP1Node::convertToAddr(string id, string port){
 string addrString = id + ":" + port;
 Address address = Address(addrString);
 return address;
}
/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
 return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}
/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
 Address joinaddr;
 *(int *)(&joinaddr.addr) = 1;
 *(short *)(&joinaddr.addr[4]) = 0;
 return joinaddr;
}
/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
 memberNode->memberList.clear();
}
/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
 printf("%d.%d.%d.%d:%d \n", addr->addr[0],addr->addr[1],addr->addr[2],
 addr->addr[3],
*(short*)&addr->addr[4]) ;
}