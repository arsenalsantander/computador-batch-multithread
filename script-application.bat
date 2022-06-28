@echo off
setlocal

set ROOT_BASE=C:\computador\computador-batch
set JAVA_HOME=%ROOT_BASE%\java\jre-11
set JAVA=%JAVA_HOME%\bin\java
	
rem #sigla
set SIGLA=xxx

rem #component
set COMPONENT_NAME=demo-computador-batch-multithread
set RUN_COMPONENT=%COMPONENT_NAME%".jar"

rem #path for component
set ROOT=%ROOT_BASE%\bin\%SIGLA%\%COMPONENT_NAME%

rem #log component
set BASE_LOG_PATH=%ROOT_BASE%\bin\%SIGLA%\%COMPONENT_NAME%\log

%JAVA% -jar %RUN_COMPONENT% --spring.profiles.active=dev

endlocal