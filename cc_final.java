package cchw_5;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.stream.Stream;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.io.*;


class seperate extends Thread {
	
    String name;
    
    long begin, finish;
    
    seperate(String name, int begin, int finish) 
    {
        this.name = name;
        
        this.begin = begin;
        
        this.finish = finish;
        
    }
    
    static void writeToFile(BufferedWriter writefile, String l1)
    {
        try {
            writefile.write(l1 + "\r\n");
            
        } catch (Exception exception) 
        {
            throw new RuntimeException(exception);
        }
    }

    public void run() 
    {
        try {
            BufferedWriter writefile = Files.newBufferedWriter(Paths.get(name));
            
            long lines = finish + 1 - begin;
            
            Stream < String > chunks = Files.lines(Paths.get(Sort1.Input_file)).skip(begin - 1).limit(lines).sorted(Comparator.naturalOrder());
            
            chunks.forEach(line -> 
            {
                writeToFile(writefile, line);
            });
            
            System.out.println(" Writing Finished " + Thread.currentThread().getName());
            
            writefile.close();
            
        } catch (Exception execption) 
        {
            System.out.println(execption);
        }
    }
}

class combine extends Thread {
	
    String chunk1, chunk2, final_chunk;
    
    combine(String chunk1, String chunk2, String final_chunk)
    {
        this.chunk1 = chunk1;
        
        this.chunk2 = chunk2;
        
        this.final_chunk = final_chunk;
    }

    public void run() 
    {
        try 
        {
            System.out.println(chunk1 + " Begin Merge " + chunk2);
            
            FileWriter w = new FileWriter(final_chunk);
            
            BufferedReader mybuffer1 = new BufferedReader(new FileReader(chunk1));
            
            BufferedReader mybuffer2 = new BufferedReader(new FileReader(chunk2));
            
            String first_line = mybuffer1.readLine();
            
            String second_line = mybuffer2.readLine();
            
            while (first_line != null || second_line != null)
            {
                if (first_line == null || (second_line != null && first_line.compareTo(second_line) > 0)) 
                {
                    w.write(second_line + "\r\n");
                    
                    second_line = mybuffer2.readLine();
                } else 
                {
                    w.write(first_line + "\r\n");
                    first_line = mybuffer1.readLine();
                }
            }
            
            System.out.println(chunk1 + " Done Merging " + chunk2);
            
            new File(chunk1).delete();
            
            new File(chunk2).delete();
            
            w.close();
        }   
        catch (Exception exec) 
        {
            System.out.println(exec);
        }
    }
}

public class Sort1 {
	
     static final String tempfiles = "C:/Users/Sai Charan/Workspace/cchw_5/temp/";
     
     static final String time = "C:/Users/Sai Charan/Workspace/cchw_5/execution/";
     
     static final String Input_file = "1GB.txt";
     
     static final String log = time + "Sort.log";

    public static void main(String[] args) throws Exception 
    {
    	
        long starting_time = System.nanoTime();
        
        int i=1,no_of_threads = 16,lines = 10737418, linesPerFile = lines / no_of_threads;   
        
        ArrayList < Thread > Running_t = new ArrayList < Thread > ();
        
        while(i<=no_of_threads) 
        {
            int begin = i == 1 ? i : (i - 1) * linesPerFile + 1;
            
            int finish = i * linesPerFile;
            
            seperate mapThreads = new seperate(tempfiles + "sort_file" + i, begin, finish);
            
            Running_t.add(mapThreads);
            
            mapThreads.start();
            
            i++;
        }
        Running_t.stream().forEach(t -> 
        {
            try 
            {
                t.join();
            } catch (Exception e) {}
        });

        int i1=0,itr,j,h=(int)(Math.log(no_of_threads) / Math.log(2));
        
        j = itr = 1;
        
        while(i1 < h)
        {
            ArrayList < Thread > running_th = new ArrayList < Thread > ();

            while ( j <= no_of_threads / (i1 + 1)) 
            {
            	
                int number = i1 * 100;
                
                String split1 = tempfiles + "sort_file" + (j + number);
                
                String split2 = tempfiles + "sort_file" + ((j + 1) + number);
                
                String sort_fileFile = tempfiles + "sort_file" + (itr + ((i1 + 1) * 100));
                
                combine th = new combine(split1, split2, sort_fileFile);
                
                running_th.add(th);
                
                th.start();
            }
            
            j+=2;
            
            itr++;
            
            running_th.stream().forEach(t -> 
            {
                try 
                {
                    t.join();
                } catch 
                (Exception exec) {}
            });
            
        }
        
        i1++;
        
        
        long finish_time = System.nanoTime();
        
        int a=10,b=9;
        
        double total_time = (finish_time - starting_time)/1e9;
        
        System.out.println(total_time);
        
        BufferedWriter time = new BufferedWriter(new FileWriter(log, true));
        
        time.write("Total Time is :" + total_time);
        
        time.close();
    }
}