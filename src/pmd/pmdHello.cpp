#include "core.hpp"
#include "ossPrimitiveFileOp.hpp"
#include "pd.hpp"

int main(int argc, char **argv)
{
	int rc = 0;
	ossPrimitiveFileOp _fileObj;
	char path[OSS_MAX_PATHSIZE + 1] = {0};

	if (argc != 2) {
		PD_LOG(PDERROR, "Invalid params, please input file path.");	
		goto error;
	}
	strncpy(path, argv[1], sizeof(path));

	rc = _fileObj.Open(path);
	
	PD_RC_CHECK(rc, PDERROR, "open %s failed. rc = %d", path, rc);
	
	rc = _fileObj.Write("Hello word");
	PD_RC_CHECK(rc, PDERROR, "write %s failed. rc = %d", path, rc);

done:
	return rc;
error:
	goto done;
}
