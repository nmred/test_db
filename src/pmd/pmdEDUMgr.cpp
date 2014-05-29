#include "pd.hpp"
#include "pmd.hpp"
#include "pmdEDUMgr.hpp"

// {{{ pmdEDUMgr::_destroyAll()

int pmdEDUMgr::_destroyAll()
{
	_setDestroyed(true);	
	setQuiesced(true);

	// stop all user edus
	unsigned int timeCounter = 0;
	unsigned int eduCount = _getEDUCount(EDU_USER);

	while (eduCount != 0) {
		if (0 == timeCounter % 50) {
			_forceEDUs(EDU_USER);	
		}
		++timeCounter;	
		ossSleepmilli(100);
		eduCount = _getEDUCount(EDU_USER);
	}

	// stop all system edus
	timeCounter = 0;
	eduCount = _getEDUCount(EDU_ALL);
	while(eduCount != 0) {
		if (0 == timeCounter % 50) {
			_forceEDUs(EDU_ALL);	
		}
		++timeCounter;	
		ossSleepmilli(100);
		eduCount = _getEDUCount(EDU_ALL);
	}

	return EDB_OK;
}

// }}}
// {{{ pmdEDUMgr::forceUserEDU()

int pmdEDUMgr::forceUserEDU(EDUID eduID)
{
	int rc = EDB_OK;
	std::map<EDUID, pmdEDUCB *>::iterator it;
	_mutex.get();
	if (isSystemEDU(eduID)) {
		PD_LOG(PDERROR, "System EDU %d can't be forced", eduID);
		rc = EDB_PMD_FORCE_SYSTEM_EDU;	
		goto error;
	}
	{
		for (it = _runQueue.begin(); it != _runQueue.end(); ++it) {
			if ((*it).second->getID() == eduID) {
				(*it).second->force();
				goto done;	
			}
		}	
		for (it = _idleQueue.begin(); it != _idleQueue.end(); ++it) {
			if ((*it).second->getID() == eduID) {
				(*it).second->force();
				goto done;	
			}
		}	
	}

done:
	_mutex.release();
	return rc;
error:
	goto done;
}

// }}}
// {{{ pmdEDUMgr::_forceEDUs()

int pmdEDUMgr::_forceEDUs(int property)
{
	std::map<EDUID, pmdEDUCB*>::iterator it;
	/*******************CRITICAL SECTION ********************/
	_mutex.get();
	for (it = _runQueue.begin(); it != _runQueue.end(); ++it) {
		if (((EDU_SYSTEM & property) && _isSystemEDU(it->first))
			|| ((EDU_USER & property) && !isSystemEDU(it->first))) {
			(*it).second->force();
			PD_LOG(PDDEBUG, "force edu[ID:%lld]", it->first);	
		}
	}

	for (it = _idleQueue.begin(); it != _idleQueue.end(); ++it) {
		if (EDU_USER & property) {
			(*it).second->force();	
		}	
	}
	_mutex.release();
	/******************END CRITICAL SECTION******************/
	
	return EDB_OK;
}

// }}}
// {{{ pmdEDUMgr::_getEDUCount()

unsigned int pmdEDUMgr::_getEDUCount(int property)
{
	unsigned int eduCount = 0;
	std::map<EDUID, pmdEDUCB*>::iterator it;
	/*******************CRITICAL SECTION ********************/
	_mutex.get_shared();
	for (it = _runQueue.begin(); it != _runQueue.end(); ++it) {
		if (((EDU_SYSTEM & property) && _isSystemEDU(it->first))
			|| ((EDU_USER & property) && !isSystemEDU(it->first))) {
			++eduCount;
		}
	}

	for (it = _idleQueue.begin(); it != _idleQueue.end(); ++it) {
		if (EDU_USER & property) {
			++eduCount;
		}	
	}
	_mutex.release_shared();
	/******************END CRITICAL SECTION******************/
	
	return eduCount;
}

// }}}
// {{{ pmdEDUMgr::postEDUPost()

int pmdEDUMgr::postEDUPost(EDUID eduID, pmdEDUEventTypes type, bool release, void *pData)
{
	int rc = EDB_OK;
	pmdEDUCB* eduCB = NULL;
	std::map<EDUID, pmdEDUCB*>::iterator it;
	_mutex.get_shared();
	if (_runQueue.end() == (it = _runQueue.find(eduID))) {
		if (_idleQueue.end() == (it = _idleQueue.find(eduID))) {
			rc = EDB_SYS;
			goto error;	
		}	
	}	

	eduCB = (*it).second;
	eduCB->postEvent(pmdEDUEvent (type, release, pData));

done:
	_mutex.release_shared();
	return rc;
error:
	goto done;
}

// }}}
// {{{ pmdEDUMgr::waitEDUPost()

int pmdEDUMgr::waitEDUPost(EDUID eduID, pmdEDUEvent &event, long long millsecond = -1) 
{
	int rc = EDB_OK;
	pmdEDUCB* eduCB = NULL;
	std::map<EDUID, pmdEDUCB*>::iterator it;
	_mutex.get_shared();
	if (_runQueue.end() == (it = _runQueue.find(eduID))) {
		if (_idleQueue.end() == (it = _idleQueue.find(eduID))) {
			rc = EDB_SYS;
			goto error;	
		}	
	}	

	eduCB = (*it).second;
	if(!eduCB->waitEvent(event, millsecond)) {
		rc = EDB_TIMEOUT;
		goto error;	
	}

done:
	_mutex.release_shared();
	return rc;
error:
	goto done;

}

// }}}
// {{{ pmdEDUMgr::returnEDU()

int pmdEDUMgr::returnEDU(EDUID eduID, bool force, bool* destroyed)
{
	int rc = EDB_OK;
	EDU_TYPES type = EDU_TYPE_UNKNOWN;
	pmdEDUCB* eduCB = NULL;
	std::map<EDUID, pmdEDUCB*>::iterator it;
	_mutex.get_shared();
	if (_runQueue.end() == (it = _runQueue.find(eduID))) {
		if (_idleQueue.end() == (it = _idleQueue.find(eduID))) {
			rc = EDB_SYS;
			*destroyed = false;
			_mutex.release_shared();
			goto error;	
		}	
	}	

	eduCB = (*it).second;
	if (eduCB) {
		type = eduCB->getType();	
	}
	_mutex.release_shared();

	if (!isPoolable(type) || force || isDestroyed()
		|| size() > (unsigned int) pmdGetKRCB()->getMaxPool())
	{
		rc = _destroyEDU(eduID);	
		if (destroyed) {
			if (EDB_OK == rc || EDB_SYS == rc) {
				*destroyed = true;
			} else {
				*destroyed = false;
			}		
		}
	} else {
		rc = _deactivateEDU(eduID);
		if (destroyed) {
			if (EDB_SYS == rc) {
				*destroyed = true;	
			} else {
				*destroyed = false;	
			}
		}	
	}

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ pmdEDUMgr::startEDU()

int pmdEDUMgr::startEDU(EDU_TYPES type, void *arg, EDUID *eduid)
{
	int rc = EDB_OK;
	EDUID eduID = 0;
	pmdEDUCB* eduCB = NULL;
	std::map<EDUID, pmdEDUCB*>::iterator it;
	
	if (isQuiesced()) {
		rc = EDB_QUIESCED;
		goto done;	
	}

	_mutex.get();
	if (true == _idleQueue.empty() || !isPoolable(type)) {
		_mutex.release();
		rc = _createNewEDU(type, arg, eduid);
		if (EDB_OK == rc) {
			goto done;	
		} else {
			goto error;	
		}
	}

	for (it = _idleQueue.begin(); (_idleQueue.end() != it) && (PMD_EDU_IDLE != (*it).second->getStatus()); it++);

	if (_idleQueue.end() == it) {
		_mutex.release();
		rc = _createNewEDU(type, arg, eduid);
		if (EDB_OK == rc) {
			goto done;	
		} else {
			goto error;	
		}
	}

	eduID = (*it).first;
	eduCB = (*it).second;
	_idleQueue.erase(eduID);
	EDB_ASSERT(isPoolable(type), "must be agent.");

	eduCB->setType(type);
	eduCB->setStatus(PMD_EDU_WAITING);
	_runQueue[eduID] = eduCB;
	*eduid = eduID;
	_mutex.release();

	eduCB->postEvent(pmdEDUEvent(PMD_EDU_EVENT_RESUME, false, arg));

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ pmdEDUMgr::_createNewEDU()

int pmdEDUMgr::_createNewEDU(EDU_TYPES type, void* arg, EDUID *eduid)
{
	int rc = EDB_OK;
	unsigned int probe	= 0;
	pmdEDUCB *cb		= NULL;
	EDUID myEDUID		= 0;
	if (isQuiesced()) {
		rc = EDB_QUIESCED;
		goto done;	
	}

	if (!getEntryFuncByType(type)) {
		PD_LOG(PDERROR, "The edu[type:%d] not exist or function is null", type);
		rc = EDB_INVALIDARG;
		probe = 30;
		goto error;	
	}

	cb = new(std::nothrow) pmdEDUCB(this, type);
	EDB_VALIDATE_GOTOERROR(cb, EDB_OOM, "Out of memery to create agent control block");
	cb->setStatus(PMD_EDU_CREATING);

	_mutex.get();
	if (_runQueue.end() != _runQueue.find(_EDUID)) {
		_mutex.release();
		rc = EDB_SYS;
		probe = 10;
		goto error;	
	}

	if (_idleQueue.end() != _idleQueue.find(_EDUID)) {
		_mutex.release();
		rc = EDB_SYS;
		probe = 15;
		goto error;	
	}

	cb->setId(_EDUID);
	if (eduid) {
		*eduid = _EDUID;	
	}

	_runQueue[_EDUID] = (pmdEDUCB*) cb;
	myEDUID = _EDUID;
	++_EDUID;
	_mutex.release();

	try {
		boost::thread agentThread(pmdEDUEntryPoint, type, cb, arg);
		agentThread.detach();	
	} catch (std::exception e) {
		_runQueue.erase(myEDUID);
		rc = EDB_SYS;
		probe = 20;
		goto error;
	}


	cb->postEvent(pmdEDUEvent(PMD_EDU_EVENT_RESUME, false, arg));

done:
	return rc;
error:
	if (cb) {
		delete cb;	
	}
	PD_LOG(PDERROR, "Failed to create new agent, probe = %d", probe);
	goto done;
}

// }}}
// {{{ pmdEDUMgr::_destroyEDU()

int pmdEDUMgr::_destroyEDU(EDUID eduID)
{
	int rc = EDB_OK;
	pmdEDUCB* eduCB = NULL;
	unsigned int eduStatus = PMD_EDU_CREATING;
	std::map<EDUID, pmdEDUCB*>::iterator it;
	std::map<unsigned int, EDUID>::iterator it1;
	_mutex.get();

	if (_runQueue.end() == (it = _runQueue.find(eduID))) {
		if (_idleQueue.end() == (it = _idleQueue.find(eduID))) {
			rc = EDB_SYS;
			goto error;	
		}

		eduCB = (*it).second;
		if (!PMD_IS_EDU_IDLE(eduCB->getStatus())) {
			rc = EDB_EDU_INVAL_STATUS;
			goto error;	
		}

		eduCB->setStatus(PMD_EDU_DESTROY);
		_idleQueue.erase(eduID);
	} else {
		eduCB = (*it).second;
		eduStatus = eduCB->getStatus();
		if (!PMD_IS_EDU_WAITING(eduStatus) && !PMD_IS_EDU_CREATING(eduStatus)) {
			rc = EDB_EDU_INVAL_STATUS;
			goto error;	
		}	
		eduCB->setStatus(PMD_EDU_DESTROY);
		_runQueue.erase(eduID);
	}

	// clean up tid/eduid map
	for (it1 = _tid_eduid_map.begin(); it1 != _tid_eduid_map.end(); ++it1) {
		if ((*it1).second == eduID) {
			_tid_eduid_map.erase(it1);	
			break;
		}
	}
	if (eduCB) {
		delete(eduCB); 
	}

done:
	_mutex.release();
	return rc;

error:
	goto done;
}

// }}}
// {{{ pmdEDUMgr::waitEDU()

int pmdEDUMgr::waitEDU(EDUID eduID)
{
	int rc			= EDB_OK;
	pmdEDUCB* eduCB = NULL;	
	unsigned int eduStatus = PMD_EDU_CREATING;
	std::map<EDUID, pmdEDUCB*>::iterator it;

	_mutex.get_shared();
	if (_runQueue.end() == (it = _runQueue.find(eduID))) {
		rc = EDB_SYS;	
		goto error;
	}

	eduCB = (*it).second;
	eduStatus = eduCB->getStatus();

	if (PMD_IS_EDU_WAITING(eduStatus)) {
		goto done;	
	}

	if (!PMD_IS_EDU_RUNNING(eduStatus)) {
		rc = EDB_EDU_INVAL_STATUS;
		goto error;	
	}

	eduCB->setStatus(PMD_EDU_WAITING);

done:
	_mutex.release_shared();
	return rc;
error:
	goto done;
}

// }}}
// {{{ pmdEDUMgr::_deactivateEDU()

int pmdEDUMgr::_deactivateEDU(EDUID eduID)
{
	int rc				   = EDB_OK;
	unsigned int eduStatus = PMD_EDU_CREATING;	
	pmdEDUCB* eduCB = NULL;
	std::map<EDUID, pmdEDUCB*>::iterator it;
	_mutex.get();
	if (_runQueue.end() == (it = _runQueue.find(eduID))) {
		if (_idleQueue.end() == (it = _idleQueue.find(eduID))) {
			goto done;	
		}
		rc = EDB_SYS;
		goto error;
	}

	eduCB = (*it).second;
	eduStatus = eduCB->getStatus();
	if (PMD_IS_EDU_IDLE(eduStatus)) {
		goto done;	
	}

	if (!PMD_IS_EDU_WAITING(eduStatus) && !PMD_IS_EDU_CREATING(eduStatus)) {
		rc = EDB_EDU_INVAL_STATUS;
		goto error;	
	}

	EDB_ASSERT(isPoolable(eduCB->getType()), "Only agent can be pooled");
	_runQueue.erase(eduID);
	eduCB->setStatus(PMD_EDU_IDLE);
	_idleQueue[eduID] = eduCB;

done:
	_mutex.release();
	return rc;
error:
	goto done;
}

// }}}
// {{{ pmdEDUMgr::activateEDU()

int pmdEDUMgr::activateEDU(EDUID eduID)
{
	int rc = EDB_OK;
	unsigned int eduStatus = PMD_EDU_CREATING;
	pmdEDUCB* eduCB = NULL;
	std::map<EDUID, pmdEDUCB*>::iterator it;
	
	_mutex.get();
	if (_idleQueue.end() == (it = _idleQueue.find(eduID))) {
		if (_runQueue.end() == (it = _runQueue.find(eduID))) {
			rc = EDB_SYS;
			goto error;	
		}	
		eduCB = (*it).second;
		eduStatus = eduCB->getStatus();
		if (PMD_IS_EDU_RUNNING(eduStatus)) {
			goto done;	
		}
		if (!PMD_IS_EDU_WAITING(eduStatus) && !PMD_IS_EDU_CREATING(eduStatus)) {
			rc = EDB_EDU_INVAL_STATUS;
			goto error;	
		}
		eduCB->setStatus(PMD_EDU_RUNNING);
		goto done;
	}	
	eduCB = (*it).second;
	eduStatus = eduCB->getStatus();
	if (PMD_IS_EDU_RUNNING(eduStatus)) {
		goto done;	
	}

	if (!PMD_IS_EDU_IDLE(eduStatus)) {
		rc = EDB_EDU_INVAL_STATUS;
		goto error;	
	}

	_idleQueue.erase(eduID);
	eduCB->setStatus(PMD_EDU_RUNNING);
	_runQueue[eduID] = eduCB;

done:
	_mutex.release();
	return rc;
error:
	goto done;
}

// }}}
// {{{ pmdEDUMgr::getEDU()

pmdEDUCB* pmdEDUMgr::getEDU(unsigned int tid)
{
	std::map<unsigned int, EDUID>::iterator it;
	std::map<EDUID, pmdEDUCB*>::iterator it1;
	EDUID eduid;
	pmdEDUCB *pResult = NULL;
	_mutex.get_shared();
	it = _tid_eduid_map.find(tid);
	if (_tid_eduid_map.end() == it) {
		pResult = NULL;	
		goto done;
	}

	eduid = (*it).second;
	it1 = _runQueue.find(eduid);
	if (_runQueue.end() != it1) {
		pResult = (*it1).second;	
		goto done;
	}
	it1 = _idleQueue.find(eduid);
	if (_idleQueue.end() != it1) {
		pResult = (*it1).second;	
		goto done;
	}

done:
	_mutex.release_shared();
	return pResult;
}

// }}}
// {{{ pmdEDUMgr::setEDU()

void pmdEDUMgr::setEDU(unsigned int tid, EDUID eduid)
{
	_mutex.get();
	_tid_eduid_map[tid] = eduid;
	_mutex.release();
}

// }}}
// {{{ pmdEDUMgr::getEDU()

pmdEDUCB* pmdEDUMgr::getEDU()
{
	return getEDU(ossGetCurrentThreadID());	
}

// }}}
// {{{ pmdEDUMgr::getEDUByID()

pmdEDUCB* pmdEDUMgr::getEDUByID(EDUID eduID)
{
	std::map<EDUID, pmdEDUCB*>::iterator it;
	pmdEDUCB *pResult = NULL;
	_mutex.get_shared();
	if (_runQueue.end() == (it = _runQueue.find(eduID))) {
		if (_idleQueue.end() == (it = _idleQueue.find(eduID))) {
			goto done;	
		}
	}
	pResult = it->second;

done:
	_mutex.release_shared();
	return pResult;
}

// }}}
