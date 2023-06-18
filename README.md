# folderTracking

This consists of two .py files - one for commercial projects and one for residential.
Used to track project status via monitoring folder info/metadata and changes.
Code is specific to how our folders were set up.
Not directly transferrable without changing folder structure or code.

Pulls data on initialization, cleans, creates db.
Sends information to google sheet, monitors folders for changes.
On change, determine what was changed, pull new info, clean, update db, update google sheet.

Learned how to use Watchdog from these two websites (combined with the documentation and some googling): 
https://philipkiely.com/code/python_watchdog.html
https://geekyhumans.com/monitor-file-changes-using-python/
