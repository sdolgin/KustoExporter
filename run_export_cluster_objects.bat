@echo off
setlocal enabledelayedexpansion

REM Parse command line arguments to find -o parameter
set "OUTPUT_DIR="
set "OTHER_ARGS="
set "PARSING_OUTPUT=0"

:parse_args
if "%1"=="" goto done_parsing
if "%1"=="-o" (
    set "PARSING_OUTPUT=1"
    shift
    goto parse_args
)
if "%1"=="--output-dir" (
    set "PARSING_OUTPUT=1"
    shift
    goto parse_args
)
if "!PARSING_OUTPUT!"=="1" (
    set "OUTPUT_DIR=%1"
    set "PARSING_OUTPUT=0"
) else (
    set "OTHER_ARGS=!OTHER_ARGS! %1"
)
shift
goto parse_args

:done_parsing

REM Set default output directory if not provided
if "!OUTPUT_DIR!"=="" set "OUTPUT_DIR=exported_schemas"

REM 1. Define venv location (sibling to this .bat)
set VENV_DIR=%~dp0\venv

REM 2. Create venv if it doesn't exist
if not exist "%VENV_DIR%" (
    echo Creating virtual environment...
    python -m venv "%VENV_DIR%"
)

REM 3. Activate venv
echo Activating virtual environment...
call "%VENV_DIR%\Scripts\activate.bat"

REM 4. Install required modules
echo Installing dependencies...
pip install azure-kusto-data

REM 5. Run the export script(s) with modified output directories
echo Running export-table-schemas.py...
python "%~dp0export-table-schemas.py" !OTHER_ARGS! -o "!OUTPUT_DIR!\Tables"

echo Running export-function-schemas.py...
python "%~dp0export-function-schemas.py" !OTHER_ARGS! -o "!OUTPUT_DIR!\Functions"

endlocal
