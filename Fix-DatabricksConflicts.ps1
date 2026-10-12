
$ErrorActionPreference = "Stop"

# ------------------------------------------------------------
# Fix Databricks Git-folder import conflicts.
# Scans .ipynb, .py, and .sql files recursively.
# Keeps .ipynb files unchanged and renames conflicting .py/.sql.
# ------------------------------------------------------------

# Find the Git repository root.
$rootOutput = & git rev-parse --show-toplevel 2>$null

if ($LASTEXITCODE -ne 0 -or -not $rootOutput) {
    throw "Cannot find the Git repository root. Run this script inside your Git repository."
}

$root = ([string]$rootOutput).Trim()
Set-Location -LiteralPath $root

$extensions = @(".ipynb", ".py", ".sql")

# Avoid committing unrelated staged changes.
& git diff --cached --quiet
if ($LASTEXITCODE -ne 0) {
    throw "You already have staged Git changes. Review or commit them before running this script."
}

# Find repository files and folders, excluding .git.
$allItems = @(
    Get-ChildItem -LiteralPath $root -Recurse -Force |
        Where-Object {
            $_.FullName -notmatch '[\\/]\.git([\\/]|$)'
        }
)

$allFiles = @(
    $allItems | Where-Object {
        -not $_.PSIsContainer -and
        $extensions -contains $_.Extension.ToLowerInvariant()
    }
)

# Group recognized files by parent folder and basename.
# Example: folder\example.ipynb, folder\example.py.
$groups = @(
    $allFiles | Group-Object {
        "$($_.DirectoryName.ToLowerInvariant())|$($_.BaseName.ToLowerInvariant())"
    }
)

# Record names already occupied by files and folders.
# Databricks may treat .ipynb, .py, and .sql with the same
# basename as conflicting notebook names.
$occupied = @{}

foreach ($item in $allItems) {
    $parentPath = Split-Path -Parent $item.FullName

    if ([string]::IsNullOrWhiteSpace($parentPath)) {
        $parentPath = $root
    }

    $parentKey = $parentPath.TrimEnd('\', '/').ToLowerInvariant()

    if ($item.PSIsContainer) {
        $name = $item.Name
    }
    elseif ($extensions -contains $item.Extension.ToLowerInvariant()) {
        $name = $item.BaseName
    }
    else {
        $name = $item.Name
    }

    $key = "$parentKey|$($name.ToLowerInvariant())"
    $occupied[$key] = $true
}

# Build a complete rename plan before changing any files.
$renamePlan = [System.Collections.Generic.List[object]]::new()

foreach ($group in $groups) {
    $members = @($group.Group)

    # Only groups containing two or more recognized files conflict.
    if ($members.Count -lt 2) {
        continue
    }

    # Keep .ipynb files unchanged. Rename conflicting .py/.sql files.
    $sources = @(
        $members | Where-Object {
            $_.Extension.ToLowerInvariant() -in @(".py", ".sql")
        }
    )

    foreach ($file in $sources) {
        if ($file.Extension -ieq ".py") {
            $kind = "Python"
        }
        else {
            $kind = "SQL"
        }

        $parentPath = $file.DirectoryName
        $parentKey = $parentPath.TrimEnd('\', '/').ToLowerInvariant()

        $newBase = "$($file.BaseName) - $kind"
        $counter = 2

        # Avoid collisions with existing files, notebooks, and folders.
        while ($occupied.ContainsKey("$parentKey|$($newBase.ToLowerInvariant())")) {
            $newBase = "$($file.BaseName) - $kind ($counter)"
            $counter++
        }

        $newName = "$newBase$($file.Extension)"
        $destination = Join-Path $parentPath $newName

        $rootPrefix = $root.TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar

        $oldRelative = $file.FullName.Substring($rootPrefix.Length).Replace('\', '/')
        $newRelative = $destination.Substring($rootPrefix.Length).Replace('\', '/')

        $renamePlan.Add([PSCustomObject]@{
            Old         = $file.FullName
            New         = $destination
            OldRelative = $oldRelative
            NewRelative = $newRelative
        })

        # Reserve the proposed name for the rest of the plan.
        $occupied["$parentKey|$($newBase.ToLowerInvariant())"] = $true
    }
}

# Show the plan without making changes.
if ($renamePlan.Count -eq 0) {
    Write-Host "No .ipynb/.py/.sql basename conflicts found." -ForegroundColor Green
    return
}

Write-Host "`nProposed filename changes:" -ForegroundColor Cyan

foreach ($item in $renamePlan) {
    Write-Host "  $($item.OldRelative)"
    Write-Host "    -> $($item.NewRelative)"
}

Write-Host "`nNo files have been changed yet." -ForegroundColor Yellow
$confirm = Read-Host "Apply these renames? Type YES to continue"

if ($confirm -cne "YES") {
    Write-Host "Cancelled. No files were renamed."
    return
}

# Rename files and stage only these rename paths.
$stagePaths = [System.Collections.Generic.List[string]]::new()

foreach ($item in $renamePlan) {
    if (-not (Test-Path -LiteralPath $item.Old -PathType Leaf)) {
        throw "Source file not found: $($item.Old). Check the repository before continuing."
    }

    if (Test-Path -LiteralPath $item.New) {
        throw "Destination already exists: $($item.New). No overwrite will be attempted."
    }

    Move-Item -LiteralPath $item.Old -Destination $item.New

    $stagePaths.Add($item.OldRelative)
    $stagePaths.Add($item.NewRelative)
}

# Pass each path as a separate argument, including paths with spaces.
$gitArgs = @("add", "-A", "--") + $stagePaths.ToArray()
& git @gitArgs

if ($LASTEXITCODE -ne 0) {
    throw "Git staging failed. Run git status --short and inspect the changes."
}

Write-Host "`nStaged changes:" -ForegroundColor Cyan
& git diff --cached --stat
& git status --short

# Commit only after explicit confirmation.
$commit = Read-Host "`nCommit these staged filename changes? Type YES to continue"

if ($commit -cne "YES") {
    Write-Host "Renames are applied and staged, but not committed."
    return
}

& git commit -m "Fix Databricks notebook filename conflicts"

if ($LASTEXITCODE -ne 0) {
    throw "Commit failed. Review Git output and run git status --short."
}

# Push only after explicit confirmation.
$push = Read-Host "`nPush the commit to origin? Type YES to continue"

if ($push -cne "YES") {
    Write-Host "Committed locally. Push later using: git push origin HEAD"
    return
}

& git push origin HEAD

if ($LASTEXITCODE -ne 0) {
    throw "Push failed. Check GitHub authentication and repository permissions."
}

Write-Host "`nCompleted: conflicting filenames renamed, committed, and pushed." -ForegroundColor Green