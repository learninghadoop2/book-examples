#/bin/env ruby 
while line = gets
           words = line.split("\t")
           words.each{ |word| puts word.strip+"\t1"}}
end 


