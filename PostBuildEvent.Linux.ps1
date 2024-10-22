param(
    $assembly
)
write-host ($assembly + ': Current path: ' + (pwd).path)
copy-item ../Lib_Linux/System.Configuration.ConfigurationManager.dll ./bin/net7.0/ -Force
copy-item ../Lib_Linux/System.Collections.Immutable.dll ./bin/net7.0/ -Force
copy-item ../FsPickler.Json/bin/Debug/net7.0/FsPickler.dll ./bin/net7.0/ -Force
copy-item ../FsPickler.Json/bin/Debug/net7.0/FsPickler.Json.dll ./bin/net7.0/ -Force

cd ./bin
try {
    $pkg = (dir "$($assembly)*.nupkg" | Sort-Object -Property Name -Descending)[0].Name
    invoke-expression "dotnet nuget push $pkg --api-key $(gc /Nuget/apikey.txt) --source https://api.nuget.org/v3/index.json  --skip-duplicate"
}
catch {
    write-host "=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+="
    write-host $_
    write-host "=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+="
}
