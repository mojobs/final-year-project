$ErrorActionPreference = "Stop"

$Pptx = "C:\Users\HP\Downloads\Final year project\Final_Year_Project_Presentation_Updated.pptx"
$Workspace = "C:\Users\HP\Downloads\Final year project\outputs\manual-20260614-ajibola-slides\presentations\proposal-slide-update"
$Pdf = Join-Path $Workspace "Final_Year_Project_Presentation_Updated.pdf"
$Preview = Join-Path $Workspace "preview_png_final"

New-Item -ItemType Directory -Force -Path $Workspace | Out-Null
New-Item -ItemType Directory -Force -Path $Preview | Out-Null

$pp = New-Object -ComObject PowerPoint.Application
$pp.Visible = [Microsoft.Office.Core.MsoTriState]::msoTrue
$pres = $pp.Presentations.Open($Pptx, $false, $false, $false)

try {
    $minFont = 999
    $issues = @()

    foreach ($slide in $pres.Slides) {
        foreach ($shape in $slide.Shapes) {
            if ($shape.HasTextFrame -and $shape.TextFrame.HasText) {
                $text = $shape.TextFrame.TextRange.Text.Trim()
                if ($text.Length -eq 0) { continue }
                $size = [double]$shape.TextFrame.TextRange.Font.Size
                if ($size -gt 0 -and $size -lt $minFont) { $minFont = $size }
                if ($size -gt 0 -and $size -lt 12) {
                    $issues += "Slide $($slide.SlideIndex): $($shape.Name) has font $size"
                }
            }
        }
    }

    $pres.SaveAs($Pdf, 32)
    $pres.SaveAs($Preview, 18)

    [pscustomobject]@{
        Slides = $pres.Slides.Count
        MinFont = $minFont
        FontIssues = $issues.Count
        Pdf = $Pdf
        Preview = $Preview
    } | Format-List

    if ($issues.Count -gt 0) {
        $issues | Set-Content -LiteralPath (Join-Path $Workspace "font-issues.txt") -Encoding UTF8
        throw "Font size audit found $($issues.Count) issue(s)."
    }
}
finally {
    $pres.Close()
    $pp.Quit()
}
