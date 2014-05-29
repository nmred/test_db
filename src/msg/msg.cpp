/**
* @file msg.cpp
* @brief	消息封装	
* @author nmred <nmred_2008@126.com>
* @version 1.0
* @date 2014-05-29
*/
#include "core.hpp"
#include "pd.hpp"
#include "msg.hpp"

using namespace bson;

// {{{ static int msgCheckBuffer()

/**
* @brief	重新分配 buffer  大小
*
* @param	ppBuffer
* @param	pBufferSize
* @param	length
*
* @return	
*/
static int msgCheckBuffer(char **ppBuffer, int *pBufferSize, int length)
{
	int rc = EDB_OK;
	if (length > *pBufferSize) {
		char *pOldBuf = *ppBuffer;
		if (length < 0) {
			PD_LOG(PDERROR, "invalid length: %d",length);
			rc = EDB_INVALIDARG;
			goto error;	
		}	
		*ppBuffer = (char *)realloc(*ppBuffer, sizeof(char) * length);
		if (!*ppBuffer) {
			PD_LOG(PDERROR, "Failed to allocate %d bytes buffer", length);
			rc = EDB_OOM;
			*ppBuffer = pOldBuf;
			goto error;	
		}
		*pBufferSize = length;
	}

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ int msgBuildReply()

/**
* @brief	构建回复的消息
*
* @param	ppBuffer
* @param	pBufferSize
* @param	returnCode
* @param	objReturn
*
* @return	
*/
int msgBuildReply(char **ppBuffer, int *pBufferSize, int returnCode, BSONObj * objReturn)
{
	int rc = EDB_OK;
	int size = sizeof(MsgReply);
	MsgReply *pReply = NULL;
	
	if (objReturn) {
		size = size + objReturn->objsize();	
	}

	rc = msgCheckBuffer(ppBuffer, pBufferSize, size);
	PD_RC_CHECK(rc, PDERROR, "Failed to realloc buffer for %d bytes, rc = %d", size , rc);

	pReply	= (MsgReply *)(*ppBuffer);
	pReply->header.messageLen   = size;
	pReply->header.opCode		= OP_REPLY;
	pReply->returnCode			= returnCode;
	pReply->numReturn			= (objReturn) ? 1 : 0;

	if (objReturn) {
		memcpy(&pReply->data[0], objReturn->objdata(), objReturn->objsize());	
	}

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ int msgExtractReply()

int msgExtractReply(char *pBuffer, int &returnCode, int &numReturn, const char **ppObjStart)
{
	int rc = EDB_OK;
	MsgReply *pReply = (MsgReply *)pBuffer;
	
	if (pReply->header.messageLen < (int)sizeof(MsgReply)) {
		PD_LOG (PDERROR, "Invalid length of reply message");
		rc = EDB_INVALIDARG;
		goto error;	
	}	

	if (pReply->header.opCode != OP_REPLY) {
		PD_LOG(PDERROR, "non-reply code is received: %d, expected %d", pReply->header.opCode, OP_REPLY);
		rc = EDB_INVALIDARG;
		goto error;	
	}

	// extract
	returnCode = pReply->returnCode;
	numReturn  = pReply->numReturn;

	if (0 == numReturn) {
		*ppObjStart = NULL;	
	} else {
		*ppObjStart = &pReply->data[0];	
	}

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ int msgBuildInsert()

/**
* @brief	构建插入的消息
*
* @param	ppBuffer
* @param	pBufferSize
* @param	objReturn
*
* @return	
*/
int msgBuildInsert(char **ppBuffer, int *pBufferSize, BSONObj &obj)
{
	int rc = EDB_OK;
	int size = sizeof(MsgInsert) + obj.objsize();
	MsgInsert *pInsert = NULL;
	
	rc = msgCheckBuffer(ppBuffer, pBufferSize, size);
	if (rc) {
		PD_LOG(PDERROR, "Failed to realloc buffer for %d bytes, rc = %d", size, rc);	
		goto error;
	}

	pInsert	= (MsgInsert *)(*ppBuffer);
	pInsert->header.messageLen   = size;
	pInsert->header.opCode		= OP_INSERT;
	pInsert->numInsert			= 1;

	memcpy(&pInsert->data[0], obj.objdata(), obj.objsize());	

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ int msgBuildInsert()

/**
* @brief	构建插入的消息
*
* @param	ppBuffer
* @param	pBufferSize
* @param	objReturn
*
* @return	
*/
int msgBuildInsert(char **ppBuffer, int *pBufferSize, vector<BSONObj*> &obj)
{
	int rc = EDB_OK;
	int size = sizeof(MsgInsert);
	MsgInsert *pInsert = NULL;
	vector<BSONObj *>::iterator it;
	char *p = NULL;
	for (it = obj.begin(); it != obj.end(); ++it) {
		size += (*it)->objsize();
	}

	rc = msgCheckBuffer(ppBuffer, pBufferSize, size);
	if (rc) {
		PD_LOG(PDERROR, "Failed to realloc buffer for %d bytes, rc = %d", size, rc);	
		goto error;
	}

	pInsert	= (MsgInsert *)(*ppBuffer);
	pInsert->header.messageLen   = size;
	pInsert->header.opCode		= OP_INSERT;
	pInsert->numInsert			= obj.size();

	p = &pInsert->data[0];
	for (it = obj.begin(); it != obj.end(); ++it) {
		memcpy(p, (*it)->objdata(), (*it)->objsize());	
		p += (*it)->objsize();
	}

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ int msgExtractInsert()

/**
* @brief	解压插入消息
*
* @param	pBuffer
* @param	numInsert
* @param	ppObjStart
*
* @return	
*/
int msgExtractInsert(char *pBuffer, int &numInsert, const char **ppObjStart)
{
	int rc = EDB_OK;
	MsgInsert *pInsert = (MsgInsert *)pBuffer;
	
	if (pInsert->header.messageLen < (int)sizeof(MsgInsert)) {
		PD_LOG (PDERROR, "Invalid length of insert message");
		rc = EDB_INVALIDARG;
		goto error;	
	}	

	if (pInsert->header.opCode != OP_INSERT) {
		PD_LOG(PDERROR, "non-insert code is received: %d, expected %d", pInsert->header.opCode, OP_INSERT);
		rc = EDB_INVALIDARG;
		goto error;	
	}

	// extract
	numInsert  = pInsert->numInsert;

	if (0 == numInsert) {
		*ppObjStart = NULL;	
	} else {
		*ppObjStart = &pInsert->data[0];	
	}

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ int msgBuildDelete()

/**
* @brief	构建删除的消息
*
* @param	ppBuffer
* @param	pBufferSize
* @param	objReturn
*
* @return	
*/
int msgBuildDelete(char **ppBuffer, int *pBufferSize, BSONObj &key)
{
	int rc = EDB_OK;
	int size = sizeof(MsgDelete) + key.objsize();
	MsgDelete *pDelete = NULL;
	
	rc = msgCheckBuffer(ppBuffer, pBufferSize, size);
	if (rc) {
		PD_LOG(PDERROR, "Failed to realloc buffer for %d bytes, rc = %d", size, rc);	
		goto error;
	}

	pDelete	= (MsgDelete *)(*ppBuffer);
	pDelete->header.messageLen   = size;
	pDelete->header.opCode		= OP_DELETE;

	memcpy(&pDelete->key[0], key.objdata(), key.objsize());	

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ int msgExtractDelete()

/**
* @brief	解压删除消息
*
* @param	pBuffer
* @param	numInsert
* @param	ppObjStart
*
* @return	
*/
int msgExtractDelete(char *pBuffer, BSONObj &key)
{
	int rc = EDB_OK;
	MsgDelete *pDelete = (MsgDelete *)pBuffer;
	
	if (pDelete->header.messageLen < (int)sizeof(MsgDelete)) {
		PD_LOG (PDERROR, "Invalid length of delete message");
		rc = EDB_INVALIDARG;
		goto error;	
	}	

	if (pDelete->header.opCode != OP_DELETE) {
		PD_LOG(PDERROR, "non-delete code is received: %d, expected %d", pDelete->header.opCode, OP_DELETE);
		rc = EDB_INVALIDARG;
		goto error;	
	}

	// extract
	key = BSONObj(&pDelete->key[0]);

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ int msgBuildQuery()

/**
* @brief	构建删除的消息
*
* @param	ppBuffer
* @param	pBufferSize
* @param	objReturn
*
* @return	
*/
int msgBuildQuery(char **ppBuffer, int *pBufferSize, BSONObj &key)
{
	int rc = EDB_OK;
	int size = sizeof(MsgQuery) + key.objsize();
	MsgQuery *pQuery = NULL;
	
	rc = msgCheckBuffer(ppBuffer, pBufferSize, size);
	if (rc) {
		PD_LOG(PDERROR, "Failed to realloc buffer for %d bytes, rc = %d", size, rc);	
		goto error;
	}

	pQuery	= (MsgQuery *)(*ppBuffer);
	pQuery->header.messageLen   = size;
	pQuery->header.opCode		= OP_QUERY;

	memcpy(&pQuery->key[0], key.objdata(), key.objsize());	

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ int msgExtractQuery()

/**
* @brief	解压删除消息
*
* @param	pBuffer
* @param	numInsert
* @param	ppObjStart
*
* @return	
*/
int msgExtractQuery(char *pBuffer, BSONObj &key)
{
	int rc = EDB_OK;
	MsgQuery *pQuery = (MsgQuery *)pBuffer;
	
	if (pQuery->header.messageLen < (int)sizeof(MsgQuery)) {
		PD_LOG (PDERROR, "Invalid length of delete message");
		rc = EDB_INVALIDARG;
		goto error;	
	}	

	if (pQuery->header.opCode != OP_QUERY) {
		PD_LOG(PDERROR, "non-query code is received: %d, expected %d", pQuery->header.opCode, OP_QUERY);
		rc = EDB_INVALIDARG;
		goto error;	
	}

	// extract
	key = BSONObj(&pQuery->key[0]);

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ int msgBuildCommand()

/**
* @brief	构建命令的消息
*
* @param	ppBuffer
* @param	pBufferSize
* @param	objReturn
*
* @return	
*/
int msgBuildCommand(char **ppBuffer, int *pBufferSize, BSONObj &obj)
{
	int rc = EDB_OK;
	int size = sizeof(MsgCommand) + obj.objsize();
	MsgCommand *pCommand = NULL;
	
	rc = msgCheckBuffer(ppBuffer, pBufferSize, size);
	if (rc) {
		PD_LOG(PDERROR, "Failed to realloc buffer for %d bytes, rc = %d", size, rc);	
		goto error;
	}

	pCommand	= (MsgCommand *)(*ppBuffer);
	pCommand->header.messageLen   = size;
	pCommand->header.opCode		= OP_COMMAND;
	pCommand->numArgs			= 1;

	memcpy(&pCommand->data[0], obj.objdata(), obj.objsize());	

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ int msgBuildCommand()

/**
* @brief	构建插入的消息
*
* @param	ppBuffer
* @param	pBufferSize
* @param	objReturn
*
* @return	
*/
int msgBuildCommand(char **ppBuffer, int *pBufferSize, vector<BSONObj*> &obj)
{
	int rc = EDB_OK;
	int size = sizeof(MsgCommand);
	MsgCommand *pCommand = NULL;
	vector<BSONObj *>::iterator it;
	char *p = NULL;
	for (it = obj.begin(); it != obj.end(); ++it) {
		size += (*it)->objsize();
	}

	rc = msgCheckBuffer(ppBuffer, pBufferSize, size);
	if (rc) {
		PD_LOG(PDERROR, "Failed to realloc buffer for %d bytes, rc = %d", size, rc);	
		goto error;
	}

	pCommand	= (MsgCommand *)(*ppBuffer);
	pCommand->header.messageLen   = size;
	pCommand->header.opCode		= OP_COMMAND;
	pCommand->numArgs			= obj.size();

	p = &pCommand->data[0];
	for (it = obj.begin(); it != obj.end(); ++it) {
		memcpy(p, (*it)->objdata(), (*it)->objsize());	
		p += (*it)->objsize();
	}

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ int msgExtractCommand()

/**
* @brief	解压插入消息
*
* @param	pBuffer
* @param	numCommand
* @param	ppObjStart
*
* @return	
*/
int msgExtractCommand(char *pBuffer, int &numArgs, const char **ppObjStart)
{
	int rc = EDB_OK;
	MsgCommand *pCommand = (MsgCommand *)pBuffer;
	
	if (pCommand->header.messageLen < (int)sizeof(MsgCommand)) {
		PD_LOG (PDERROR, "Invalid length of insert message");
		rc = EDB_INVALIDARG;
		goto error;	
	}	

	if (pCommand->header.opCode != OP_COMMAND) {
		PD_LOG(PDERROR, "non-command code is received: %d, expected %d", pCommand->header.opCode, OP_COMMAND);
		rc = EDB_INVALIDARG;
		goto error;	
	}

	// extract
	numArgs  = pCommand->numArgs;

	if (0 == numArgs) {
		*ppObjStart = NULL;	
	} else {
		*ppObjStart = &pCommand->data[0];	
	}

done:
	return rc;
error:
	goto done;
}

// }}}
