# Release Workflow

## GO

It's a manual Workflow with the following commands:

```
git tag sdks/go/v1.4.0
git push origin sdks/go/v1.4.0
go list -m github.com/bytefish/SqlFlow/sdks/go@v1.4.0
```

## .NET

The Packages are released by:

1. Built to `sdks\dotnet\nuget` 
2. Put into a folder with the matching version number
3. Manually uploaded using the NuGet interface

## Java

