$m = "Version - $(Get-Date -Format g)"
git checkout master
.\mcpp.exe -k -P -o ..\src\qpi.sql -D AZURE -D MI qpi.tmpl.sql
git commit -a -m "$m"
git checkout azure-db
.\mcpp.exe -k -P -o ..\src\qpi.sql -D AZURE -D DB qpi.tmpl.sql
git commit -a -m "$m"
git checkout sql2017
.\mcpp.exe -k -P -o ..\src\qpi.sql -D SQL2017 qpi.tmpl.sql
git commit -a -m "$m"
git checkout sql2016
.\mcpp.exe -k -P -o ..\src\qpi.sql -D SQL2016 qpi.tmpl.sql
git commit -a -m "$m"