# glow

Ideally, job can be run on a big machine. For example, "sort | uniq | wc" for word count for large files. 
However, with super large files, doing the same thing distributedly requires lots of plumbing effort.

Glow aims to make it easy to distribute work to remote machines.

What's special?
 1. If any task can be done by unix pipe style, it can be done by Glow.
 2. Glow provides tools to distribute tasks: map by hashing, reduce by grouping.
 3. Each command is a json sent to leader. So more powerful tools can build on them.

What is the architecture?
 1. Each participating machine installs an agent talking to a leader.
 2. On any computer, user submits a job to leader.
 3. Agent picks up one role of the job, starts the process, setups the input/output. All participating agents thus forms a pipeline.
 4. user feed data into the pipeline.
 5. When all results arrived, user shuts down the pipeline, or leaves it running continuously by feeding more data.

What is the pipeline?
 1. Pipeline can flow 1~1, 1~m, 1~m~n
 2. pipeline can ajust flow ratio to scale up or down 1~m to 1~m*n or n~m, on demand.

