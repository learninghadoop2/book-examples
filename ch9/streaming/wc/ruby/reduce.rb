#!/usr/bin/env ruby         

current = nil
 count = 0
 while line = gets
word, counter = line.split("\t")
if word == current
count = count+1
else
puts current+"\t"+count.to_s if current
current = word
count = 1
 end end         
puts current+"\t"+count.to_s

