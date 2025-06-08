@echo off
chcp 65001 >nul
set PYTHONUTF8=1
set PYTHONUNBUFFERED=1
:loop
echo [%date% %time%] Launching a Telegram bot...
python -u bot.py
echo [%date% %time%] Bot is dead. Restarting in 5 seconds...
timeout /t 5 /nobreak >nul
goto loop