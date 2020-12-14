.\mcpp.exe -k -P -o ..\src\qpi.sql -D AZURE -D MI qpi.tmpl.sql
.\mcpp.exe -k -P -o .\azure-db\qpi.sql -D AZURE -D DB qpi.tmpl.sql
.\mcpp.exe -k -P -o .\sql2017\qpi.sql -D SQL2017 qpi.tmpl.sql
.\mcpp.exe -k -P -o .\sql2016\qpi.sql -D SQL2016 qpi.tmpl.sql
.\mcpp.exe -k -P -o .\az-dw\qpi.sql -D AzDw qpi.tmpl.sql