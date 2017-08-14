#!/usr/bin/gnuplot

reset
#fontsize = 12
set terminal pngcairo crop size 1000,640
set output "bench/bench.png"

#set key at graph 0.24, 0.8 horizontal samplen 0.1

set key at graph 0.0, 0.8 horizontal samplen 0.1

set style data histogram
set style histogram errorbars gap 2 lw 1
set style fill solid 1.00 border 0
set boxwidth 0.8
set xtic rotate
unset ytics
set y2tics rotate by 90

#set yrange [0:100];

set y2label 'Time (seconds)' offset -2.5
set xlabel ' '
set size 1, 1

set label 1 'goofys' at graph -0.75, 0.8 left rotate by 90
set label 2 's3fs' at graph -0.45, 0.8 left rotate by 90
set label 3 'riofs' at graph -0.13, 0.8 left rotate by 90

set lmargin at screen 0.1

set datafile separator "\t"

set multiplot #layout 1,3
set bmargin at screen 0.4
#set size 1, 1

set origin 0.0,0.1
set size 0.31,0.8
set xrange [5.5:7.8]

plot 'bench/bench.data' using 2:3:4 title " ", \
     '' using 5:6:7 title " ", \
     '' using 8:9:10 title " ", \
     '' using 0:(0):xticlabel(1) w l title ''

set key off
unset label 1
unset label 2
unset label 3
set lmargin

set origin 0.255,0.1
set size 0.5,0.8
set xrange [-1:5.8]

plot 'bench/bench.data' using 2:3:4 title " ", \
     '' using 5:6:7 title " ", \
     '' using 8:9:10 title " ", \
     '' using 0:(0):xticlabel(1) w l title ''

set origin 0.7,0.1
set size 0.175,0.8
set xrange [7.5:8.7]
set yrange [0:1.1]

plot 'bench/bench.data' using 2:3:4 title " ", \
     '' using 5:6:7 title " ", \
     '' using 8:9:10 title " ", \
     '' using 0:(0):xticlabel(1) w l title ''

unset multiplot
