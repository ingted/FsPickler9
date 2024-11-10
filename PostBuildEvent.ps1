param(
    $assembly = "FAkka.FsPickler"
)


function Get-VersionFromFileName {
    param (
        [string]$fileName
    )

    # 提取類似於 "1.0"、"10.2.3"、"3.4.5.6" 等的版本號
    if ($fileName -match '\d+(\.\d+)+') {
        return [version]$matches[0] # 轉換為 [version] 類型進行數值比較
    } else {
        return [version]'0.0' # 如果找不到版本號，則返回最低版本
    }
}

function Get-LibPacksContent {
    $currentDir = Get-Location

    while ($currentDir -ne [System.IO.Path]::GetPathRoot($currentDir)) {
        $libPacksPath = Join-Path -Path $currentDir -ChildPath 'lib-packs.txt'
        if (Test-Path $libPacksPath) {
            return Get-Content -Path $libPacksPath -Raw
        }
        $currentDir = (Get-Item $currentDir).Parent.FullName
    }

    Write-Host "lib-packs.txt not found in any parent directory." -ForegroundColor Red
    return $null
}


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
    $pkg = (dir "$($assembly)*.nupkg" | Sort-Object -Property { Get-VersionFromFileName $_.Name } -Descending)[0]
    copy $pkg.FullName $(Get-LibPacksContent)
    $pkgName = $pkg.Name
    invoke-expression "dotnet nuget push $pkgName --api-key $(gc G:\Nuget\apikey.txt) --source https://api.nuget.org/v3/index.json  --skip-duplicate"
}
catch {
    write-host "=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+="
    write-host $_
    write-host "=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+="
}
