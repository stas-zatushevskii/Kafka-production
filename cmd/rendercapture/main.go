package main

import (
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/image/font"
	"golang.org/x/image/font/basicfont"
	"golang.org/x/image/math/fixed"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "Usage: rendercapture <input.txt> <output.png> <title>")
		os.Exit(1)
	}

	inputPath := os.Args[1]
	outputPath := os.Args[2]
	title := os.Args[3]

	content, err := os.ReadFile(inputPath)
	fatalIfErr(err)

	lines := strings.Split(strings.TrimRight(string(content), "\n"), "\n")
	if len(lines) == 1 && lines[0] == "" {
		lines = []string{"<empty>"}
	}

	padding := 24
	lineHeight := 20
	titleHeight := 28
	maxChars := maxLineLength(append([]string{title}, lines...))
	width := padding*2 + maxChars*8
	if width < 900 {
		width = 900
	}
	height := padding*2 + titleHeight + lineHeight*(len(lines)+1)

	img := image.NewRGBA(image.Rect(0, 0, width, height))
	draw.Draw(img, img.Bounds(), &image.Uniform{C: color.RGBA{0x11, 0x18, 0x27, 0xff}}, image.Point{}, draw.Src)

	cardRect := image.Rect(16, 16, width-16, height-16)
	draw.Draw(img, cardRect, &image.Uniform{C: color.RGBA{0x0b, 0x12, 0x20, 0xff}}, image.Point{}, draw.Src)

	drawText(img, padding, padding+16, title, color.RGBA{0x93, 0xc5, 0xfd, 0xff})

	y := padding + titleHeight + 6
	for _, line := range lines {
		drawText(img, padding, y, line, color.RGBA{0xe5, 0xe7, 0xeb, 0xff})
		y += lineHeight
	}

	fatalIfErr(os.MkdirAll(filepath.Dir(outputPath), 0o755))
	file, err := os.Create(outputPath)
	fatalIfErr(err)
	defer file.Close()

	fatalIfErr(png.Encode(file, img))
}

func drawText(img *image.RGBA, x, y int, text string, textColor color.Color) {
	d := &font.Drawer{
		Dst:  img,
		Src:  image.NewUniform(textColor),
		Face: basicfont.Face7x13,
		Dot:  fixed.P(x, y),
	}
	d.DrawString(text)
}

func maxLineLength(lines []string) int {
	maxLen := 0
	for _, line := range lines {
		if len(line) > maxLen {
			maxLen = len(line)
		}
	}
	return maxLen
}

func fatalIfErr(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
