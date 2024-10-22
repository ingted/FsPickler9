param(
    $assembly
)
write-host ($assembly + ': Current path: ' + (pwd).path)
copy-item ..\Lib_Windows\System.Configuration.ConfigurationManager.dll .\bin\net9.0\ -Force
copy-item ..\Lib_Windows\System.Collections.Immutable.dll .\bin\net9.0\ -Force
#try{
#    copy-item ..\FsPickler.Json\bin\net9.0\FsPickler.dll .\bin\net9.0\ -Force 
#    copy-item ..\FsPickler.Json\bin\net9.0\FsPickler.Json.dll .\bin\net9.0\ -Force
#}catch {
#
#}

cd ./bin
try {
    $pkg = (dir "$($assembly)*.nupkg" | Sort-Object -Property Name -Descending)[0].Name
    invoke-expression "dotnet nuget push $pkg --api-key $(gc G:\Nuget\apikey.txt) --source https://api.nuget.org/v3/index.json  --skip-duplicate"
}
catch {
    write-host "=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+="
    write-host $_
    write-host "=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+="
}
