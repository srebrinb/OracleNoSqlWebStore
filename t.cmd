@echo off
call :treeProcess
goto :eof

:treeProcess
rem Do whatever you want here over the files of this subdir, for example:
for %%f in (*.jpg) do curl -X PUT "http://127.0.0.1:5055/" --data-binary "@%%f" -H "Content-Type: image/jpeg"
for /D %%d in (*) do (
    cd %%d
    call :treeProcess
    cd ..
)
exit /b