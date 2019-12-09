MP3 by Haoyu Zhang (haoyuz3) and Haorong Sun (haorong4)

To start introducer (start group):

	cd introducer/
	go run introducer.go

To start member (join group):
	
	cd member/
	go run member.go

While running:
	
	To quit group: ctrl + \
	To print member list: m + enter

Distributed file system command:

    Upload file:                                   put localfilename sdfsfilename
    Download file:                                 get sdfsfilename localfilename
    Delete file:                                   delete sdfsfilename
    List replicated files:                         store
    List all VM addresses replicated this file:    ls ls sdfsfilename
	
Have a nice day! :D