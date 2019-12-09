MP4 by Haoyu Zhang (haoyuz3) and Haorong Sun (haorong4)

To start introducer (start group):

	cd introducer/
	go run introducer.go

To start member (join group):
	
	cd member/
	go run member.go

Distributed file system command:

    Upload file:                                   put localfilename sdfsfilename
    Download file:                                 get sdfsfilename localfilename
    Delete file:                                   delete sdfsfilename
    List replicated files:                         store
    List all VM addresses replicated this file:    ls ls sdfsfilename
    Load all files under the directory 'dir'       every dir 

MapleJuice System command:
    maple <executable> <num_of_worker> <prefix_of_inter_file> <prefix_of_data_file>
    juice <executable> <num_of_worker> <prefix_of_inter_file> <name_of_destination_file> <delete>

WordCount example:
    start all the machine with 'go run introducer.go' on vm1 and 'go run member.go' on others
    type the following command in one of the machine:
        
    maple:
        every ../wordFiles
        put word word
        maple word 10 ff file

    juice:
        put jwc jwc
        juice jwc 10 ff dest 0
    the result file dest will store in 4 of the machines under introducer/files/ or member/files folder.

	
Have a nice day! :D