/**
* @file pd.cpp
* @brief	日志写入
* @author nmred <nmred_2008@126.com>
* @version 1.0
* @date 2014-05-27
*/
#include "core.hpp"
#include "pd.hpp"
#include "ossLatch.hpp"
#include "ossPrimitiveFileOp.hpp"

const static char *PDLEVELSTRING[] = 
{
	"SERVERE",
	"ERROR",
	"EVENT",
	"WARNING",
	"INFO",
	"DEBUG"	
};

const static char *PD_LOG_HEADER_FORMAT = "%04d-%02d-%02d-%02d.%02d.%02d.%06d\
					\
Level:%s"OSS_NEWLINE"PID:%-37dTID:%d"OSS_NEWLINE"Function:%-32sLine:%d"\
OSS_NEWLINE"File:%s"OSS_NEWLINE"Message:%s"OSS_NEWLINE OSS_NEWLINE;

PDLEVEL _curPDLevel = PD_DFT_DIAGLEVEL;
char _pdDiagLogPath[OSS_MAX_PATHSIZE + 1] = {0};

ossXLatch _pdLogMutex;
ossPrimitiveFileOp _pdLogFile;

// {{{ const char *getPDLevelDesp()

/**
* @brief	获取对应日志等级的描述信息 
*
* @param	level
*
* @return   char * 日志描述信息	
*/
const char *getPDLevelDesp(PDLEVEL level)
{
	if ((unsigned int)level > (unsigned int)PDDEBUG) {
		return "Unknow Level";	
	}	

	return PDLEVELSTRING[(unsigned int)level];
}

// }}}
// {{{ static int _pdLogFileReopen()

/**
* @brief	打开日志文件句柄 
*
* @return	int	
*/
static int _pdLogFileReopen()
{
	int rc = EDB_OK;
	_pdLogFile.Close();
	rc = _pdLogFile.Open(_pdDiagLogPath);
	if (rc) {
		printf("Failed to open log file, errno = %d"OSS_NEWLINE, rc);
		goto error;	
	}
	_pdLogFile.seekToEnd();

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ static int _pdLogFileWrite()

/**
* @brief	写入日志文件 
*
* @param	const char *pData
*
* @return	int rc	
*/
static int _pdLogFileWrite(const char *pData)
{
	int rc = EDB_OK;
	size_t dataSize = strlen(pData);
	_pdLogMutex.get();
	if (!_pdLogFile.isValid()) {
		rc = _pdLogFileReopen();
		if (rc) {
			printf("Failed to open log file, errno = %d"OSS_NEWLINE, rc);
			goto error;	
		}
	}

	rc = _pdLogFile.Write(pData, dataSize);
	if (rc) {
		printf("Failed to write into log file, errno = %d"OSS_NEWLINE, rc);
		goto error;	
	}

done:
	_pdLogMutex.release();
	return rc;
error:
	goto done;
}

// }}}
// {{{ pdLog()

/**
* @brief	记录日志 
*
* @param	level	日志级别
* @param	func	函数名
* @param	file	文件名
* @param	line	文件行数
* @param	format	格式化字符串方法
* @param	...
*/
void pdLog(PDLEVEL level, const char *func, const char *file, unsigned int line, const char *format, ...)
{
	int rc = EDB_OK;
	if (_curPDLevel < level) {
		return;	
	}
	va_list ap;
	char userInfo[PD_LOG_STRINGMAX];
	char sysInfo[PD_LOG_STRINGMAX];
	struct tm otm;
	struct timeval tv;
	struct timezone tz;
	time_t tt;

	gettimeofday(&tv, &tz);
	tt = tv.tv_sec;
	localtime_r (&tt, &otm);

	va_start(ap, format);
	vsnprintf(userInfo, PD_LOG_STRINGMAX, format, ap);
	va_end(ap);

	snprintf(sysInfo, PD_LOG_STRINGMAX, PD_LOG_HEADER_FORMAT,
			 otm.tm_year + 1900,
			 otm.tm_mon + 1,
			 otm.tm_mday,
			 otm.tm_hour,
			 otm.tm_min,
			 otm.tm_sec,
			 tv.tv_usec,
			 PDLEVELSTRING[level],
			 getpid(),
			 syscall(SYS_gettid),
			 func,
			 line,
			 file,
			 userInfo
	);
	printf("%s"OSS_NEWLINE, sysInfo);
	if (_pdDiagLogPath[0] != '\0') {
		rc = _pdLogFileWrite(sysInfo);
		if (rc) {
			printf("Failed to write into log file, errno = %d"OSS_NEWLINE, rc);
			printf("%s"OSS_NEWLINE, sysInfo);	
		}
	}

	return;
}

// }}}
