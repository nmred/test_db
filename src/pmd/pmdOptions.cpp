#include "pmdOptions.hpp"
#include "pd.hpp"

// {{{ pmdOptions::pmdOptions()

pmdOptions::pmdOptions()
{
	memset(_dbPath, 0, sizeof(_dbPath));
	memset(_logPath, 0, sizeof(_logPath));
	memset(_confPath, 0, sizeof(_confPath));
	memset(_svcName, 0, sizeof(_svcName));
	_maxPool = NUMPOOL;
}

// }}}
// {{{ pmdOptions::~pmdOptions()

pmdOptions::~pmdOptions()
{
	
}

// }}}
// {{{ pmdOptions::readCmd()

int pmdOptions::readCmd(int argc, char **argv,
						po::options_description &desc,
						po::variables_map &vm)
{
	int rc = EDB_OK;
	try {
		po::store(po::command_line_parser(argc, argv).options(desc).allow_unregistered().run(), vm);
		po::notify(vm);	
	} catch (po::unknown_option &e) {
		std::cerr << "Unknown arguments:"
			      << e.get_option_name() << std::endl;
		rc = EDB_INVALIDARG;
		goto error;
	} catch (po::invalid_option_value &e) {
		std::cerr << "Invalid arguments:"
			      << e.get_option_name() << std::endl;
		rc = EDB_INVALIDARG;
		goto error;
	} catch (po::error &e) {
		std::cerr << "Error: "
			      << e.what() << std::endl;
		rc = EDB_INVALIDARG;
		goto error;
	}

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ pmdOptions::importVM()

int pmdOptions::importVM(const po::variables_map &vm, bool isDefault = true)
{
	int rc = EDB_OK;
	const char *p = NULL;
	
	if (vm.count (PMD_OPTION_CONFPATH)) {
		p = vm[PMD_OPTION_CONFPATH].as<string>().c_str();
		strncpy(_confPath, p, OSS_MAX_PATHSIZE);	
	} else if (isDefault) {
		strcpy(_confPath, "./"CONFFILENAME);	
	}
	
	if (vm.count (PMD_OPTION_LOGPATH)) {
		p = vm[PMD_OPTION_LOGPATH].as<string>().c_str();
		strncpy(_logPath, p, OSS_MAX_PATHSIZE);	
	} else if (isDefault) {
		strcpy(_logPath, "./"LOGFILENAME);	
	}
	
	if (vm.count (PMD_OPTION_DBPATH)) {
		p = vm[PMD_OPTION_DBPATH].as<string>().c_str();
		strncpy(_dbPath, p, OSS_MAX_PATHSIZE);	
	} else if (isDefault) {
		strcpy(_dbPath, "./"DBFILENAME);	
	}
	
	if (vm.count (PMD_OPTION_SVCNAME)) {
		p = vm[PMD_OPTION_SVCNAME].as<string>().c_str();
		strncpy(_svcName, p, OSS_MAX_PATHSIZE);	
	} else if (isDefault) {
		strcpy(_svcName, SVCNAME);	
	}
	
	if (vm.count (PMD_OPTION_MAXPOOL)) {
		_maxPool = vm[PMD_OPTION_MAXPOOL].as<unsigned int>();
	} else if (isDefault) {
		_maxPool = NUMPOOL;
	}

	return rc;
}
// }}}
// {{{ pmdOptions::readConfigureFile()

int pmdOptions::readConfigureFile(const char *path,
								  po::options_description &desc,
								  po::variables_map &vm)
{
	int rc = EDB_OK;
	char conf[OSS_MAX_PATHSIZE + 1] = {0};
	strncpy(conf, path, OSS_MAX_PATHSIZE);
	try {
		po::store(po::parse_config_file<char>(conf, desc, true), vm);
		po::notify(vm);	
	} catch (po::reading_file) {
		std::cerr << "Failed to open config file: "
			      << (std::string) conf << std::endl
				  << "Using defailt settings" << std::endl;
		rc = EDB_IO;
		goto error;
	} catch (po::unknown_option &e) {
		std::cerr << "Unknown arguments:"
			      << e.get_option_name() << std::endl;
		rc = EDB_INVALIDARG;
		goto error;
	} catch (po::invalid_option_value &e) {
		std::cerr << "Invalid arguments:"
			      << e.get_option_name() << std::endl;
		rc = EDB_INVALIDARG;
		goto error;
	} catch (po::error &e) {
		std::cerr << "Error: "
			      << e.what() << std::endl;
		rc = EDB_INVALIDARG;
		goto error;
	}

done:
	return rc;
error:
	goto done;
}

// }}}
// {{{ pmdOptions::init()

int pmdOptions::init(int argc, char **argv)
{
	int rc = EDB_OK;
	po::options_description all("Command options");
	po::variables_map vm;
	po::variables_map vm2;
	
	PMD_ADD_PARAM_OPTIONS_BEGIN(all)
		PMD_COMMANDS_OPTIONS
	PMD_ADD_PARAM_OPTIONS_END
	rc = readCmd(argc, argv, all, vm);
	if (rc) {
		PD_LOG(PDERROR, "Failed to read cmd, rc = %d", rc);
		goto error;	
	}

	if (vm.count(PMD_OPTION_HELP)) {
		std::cout << all << std::endl;
		rc = EDB_PMD_HELP_ONLY;
		goto done;	
	}

	if (vm.count(PMD_OPTION_CONFPATH)) {
		rc = readConfigureFile(vm[PMD_OPTION_CONFPATH].as<string>().c_str(), all, vm2);	
	}
	if (rc) {
		PD_LOG(PDERROR, "unexpected error when reading conf file, rc = %d", rc);	
		goto error;
	}

	importVM(vm2);
	if (rc) {
		PD_LOG(PDERROR, "failed to import from vm2, rc = %d", rc);
		goto error;	
	}

	rc = importVM(vm);
	if (rc)  {
		PD_LOG(PDERROR, "failed to import from vm, rc = %d", rc);
		goto error;	
	}

done:
	return rc;
error:
	goto done;
}

// }}}
