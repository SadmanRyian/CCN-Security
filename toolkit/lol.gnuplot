set xtics rotate by -45
set style data histogram
set style fill solid border -1
set rmargin 10
set yrange [0:1]
set terminal png
plot 'lol.values' using 2:xtic(1) title "delivery_prob"
