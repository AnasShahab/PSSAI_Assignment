import java.util.*;
import java.io.*;

public class JobSchedule{
    public static void main(String[] args)throws Exception{
		/********** File Read **********/
    	
		int totalJobs, totalMach;
		Scanner s = new Scanner(new File("input.txt"));
		totalJobs = Integer.parseInt(s.next());
		totalMach = Integer.parseInt(s.next());
		s.nextLine();

		Scanner sc = new Scanner(new File("input.txt"));
		sc.nextLine();
		ArrayList<List<Integer>> subjobList = new ArrayList<List<Integer>>();
		int ordernum = 0, jobnum = 0;
		while (sc.hasNext()) {
			ArrayList<Integer> subJob = new ArrayList<Integer>();
			subJob.add(jobnum);
			subJob.add(ordernum);
			subJob.add(Integer.parseInt(sc.next()));
			subJob.add(Integer.parseInt(sc.next()));
			subjobList.add(subJob);
			ordernum++;
			if ((subjobList.size()) % totalMach == 0) {
				ordernum = 0;
				jobnum++;
			}
		}
		sc.close();
		
		/********** putting things into objects **********/
		
		int jobIDNum = -1;
		for(int i=0; i<subjobList.size();i++) {
			List<Integer> sj = subjobList.get(i);
			if(sj.get(0)!=jobIDNum) {
				jobIDNum = sj.get(0);
				Job j = new Job(sj.get(0),new ArrayList<Subjob>());
				j.subjobs.add(new Subjob(sj.get(0), sj.get(1), sj.get(2), sj.get(3)));
				Job.jobs.add(j);
			}else {
				Job j = Job.jobs.get(sj.get(0));
				j.subjobs.add(new Subjob(sj.get(0), sj.get(1), sj.get(2), sj.get(3)));
			}
		}
		
		/**************A Small Example***************/
    	/*
        Subjob sj00 = new Subjob(0, 0, 0, 5);
        Subjob sj01 = new Subjob(0, 1, 2, 1);
        Subjob sj02 = new Subjob(0, 2, 1, 4);
        ArrayList<Subjob> sjList0 = new ArrayList<Subjob>();
        sjList0.add(sj00);
        sjList0.add(sj01);
        sjList0.add(sj02);

        Subjob sj10 = new Subjob(1, 0, 0, 3);
        Subjob sj11 = new Subjob(1, 1, 1, 2);
        Subjob sj12 = new Subjob(1, 2, 2, 1);
        ArrayList<Subjob> sjList1 = new ArrayList<Subjob>();
        sjList1.add(sj10);
        sjList1.add(sj11);
        sjList1.add(sj12);

        Subjob sj20 = new Subjob(2, 0, 1, 1);
        Subjob sj21 = new Subjob(2, 1, 2, 3);
        Subjob sj22 = new Subjob(2, 2, 0, 2);
        ArrayList<Subjob> sjList2 = new ArrayList<Subjob>();
        sjList2.add(sj20);
        sjList2.add(sj21);
        sjList2.add(sj22);

        Job j0 = new Job(0, sjList0);
        Job j1 = new Job(1, sjList1);
        Job j2 = new Job(2, sjList2);

        Job.jobs.add(j0);
        Job.jobs.add(j1);
        Job.jobs.add(j2);
		*/
		/*****maxTime: carry out each subjob one by one*****/
		
		Integer maxTime = 0;
        for(Integer j=0; j<Job.jobs.size(); j++) {
        	Job job = Job.jobs.get(j);
        	for(Integer s_index =0; s_index<job.subjobs.size();s_index++) {
        		Subjob sj = job.subjobs.get(s_index);
        		maxTime = maxTime + sj.time;
        	}
        }
        
        System.out.println("Max Time = "+maxTime+"\n");
		
		Integer numOfChanges = (int) Math.round(totalJobs * totalMach * 0.2);
        //Integer numOfChanges = 1;
		
		ArrayList<Queue> queues = Queue.initalizeQueues(Job.jobs);
		ArrayList<Queue> theQueues = queues;
		Queue.printQueues(theQueues);
        Integer time = completionTime(theQueues, maxTime);
        System.out.println("Initial schedule with time: "+time+"\n");
        
        Integer roundsOfImprovement = 50;
        double temperature = 1000;
        for(int round=0; round<roundsOfImprovement; round++) {
        	System.out.println("round: "+round+" with temperature = "+temperature);
        	ArrayList<Queue> queues1 = Queue.neighbor(theQueues, numOfChanges);
            Integer newTime = completionTime(queues1, maxTime);
            while(newTime>maxTime) {
            	queues1 = Queue.neighbor(theQueues, numOfChanges);
            	newTime = completionTime(queues1, maxTime);
            	//Queue.printQueues(theQueues);
            	//System.out.println("here");
            }
            if(newTime<time) {
            	theQueues = queues1;
            	time = newTime;
            	Queue.printQueues(theQueues);
                System.out.println("time: "+time+" Better new Schedule!!!\n");
            }else {
            	if(largerThantheRandomNum(temperature, time, newTime)) {
            		theQueues = queues1;
            		time = newTime;
            		Queue.printQueues(theQueues);
            		System.out.println("time: "+time+" Not better but still chosen by chance\n");
            	}else {
            		System.out.println("keep the old one!");
            	}
            }
            temperature= temperature/(round+1.5);
        }
	}
    
    public static Integer completionTime(ArrayList<Queue> allQueues, Integer maxTime){
        Integer time = 0;
        Integer numOfAllSubjobs = 0;
        Integer numOfSubjobsDone = 0;
        Integer numOfJobs = Job.jobs.size();
        Integer numOfMachines = allQueues.size();

        for(Integer j=0; j<numOfJobs; j++)
            numOfAllSubjobs = numOfAllSubjobs + Job.jobs.get(j).subjobs.size();

        ArrayList<Integer> timeToNextSubjob = new ArrayList<Integer>();
        for(Integer m = 0; m < numOfMachines; m++)
            timeToNextSubjob.add(m,0);
        
        ArrayList<Integer> posInQueue = new ArrayList<Integer>(); //the position of the subjob being/to be processed in the queue of machine m
        for(Integer m = 0; m < numOfMachines; m++)
            posInQueue.add(m,0);

        ArrayList<Integer> jobProgress = new ArrayList<Integer>();
        for(Integer j = 0; j < numOfJobs; j++)
            jobProgress.add(j,0);
        
        //System.out.println("numOfAllSubjobs: "+numOfAllSubjobs);
        
        while(numOfAllSubjobs > numOfSubjobsDone && time < maxTime+1){
            if(anyFreeMachine(timeToNextSubjob)){
                for(Integer m = 0; m < numOfMachines; m++){
                    if(posInQueue.get(m)<allQueues.get(m).permutation.size()){ //If there are still subjobs left for Machine m to work on
                        Subjob sj = Queue.subjobInAllQueues(m, posInQueue.get(m), allQueues);
                        Integer orderInJob = sj.orderInJob;
                        if(isFreeMachine(m, timeToNextSubjob) && orderInJob==jobProgress.get(sj.jobID)){
                            timeToNextSubjob = setTimeToNextSubjob(m, sj.time, timeToNextSubjob);
                            jobProgress.set(sj.jobID, orderInJob);
                        }
                    }
                }
            }
            ArrayList<Integer> oldTimeToNextSubjob = new ArrayList<Integer>(timeToNextSubjob);
            timeToNextSubjob = updateAllTimeToNextSubjob(-1, timeToNextSubjob); //all machines do their subjobs, if any, with time unit = 1
            for (Integer m = 0; m < numOfMachines; m++) {
                if (oldTimeToNextSubjob.get(m) != 0) {
                    if (isFreeMachine(m, timeToNextSubjob) && posInQueue.get(m) < allQueues.get(m).permutation.size()) {
                        Integer oldPos = posInQueue.get(m);
                        Subjob sj = Queue.subjobInAllQueues(m, oldPos, allQueues);
                        if (sj.orderInJob == jobProgress.get(sj.jobID))
                            posInQueue.set(m, oldPos + 1);
                        jobProgress.set(sj.jobID, sj.orderInJob + 1);
                        numOfSubjobsDone++;
                    }
                }
            }
            time++;
            //System.out.println("Time: "+time+"   numOfSubjobsDone: "+numOfSubjobsDone);
            //printALI(posInQueue, "posInQueue", 1);
            //printALI(timeToNextSubjob, "timeToNextSubjob", 1);
            //printALI(jobProgress, "jobProgress", 1);
            //System.out.println("**********");

        }
        return time;
    }

    public static void printALI(ArrayList<Integer> list, String str, Integer s){
        System.out.print(str+": [");
        for(int i=0;i<list.size();i++){
            if(i!=list.size()-1){
                System.out.print(list.get(i)+" ");
            }else{
                System.out.print(list.get(i));
            }
        }
        System.out.print("]");
        if(s==0){
            System.out.print("\t");
        }else if(s==1){
            System.out.println();
        }
    }
    
    public static Boolean isFreeMachine(Integer m, ArrayList<Integer> timeToNextSubjob){
        return timeToNextSubjob.get(m)==0;
    }

    public static Boolean anyFreeMachine(ArrayList<Integer> timeToNextSubjob){
        for(Integer m = 0; m < timeToNextSubjob.size(); m++)
            if(timeToNextSubjob.get(m)==0)
                return true;
        return false;
    }

    public static ArrayList<Integer> updateAllTimeToNextSubjob(Integer change, ArrayList<Integer> timeToNextSubjob){
        for(Integer m = 0; m < timeToNextSubjob.size(); m++){
            Integer oldTime = timeToNextSubjob.get(m);
            if(oldTime>0)
                timeToNextSubjob.set(m, oldTime+change);
        }
        return timeToNextSubjob;
    }

    public static ArrayList<Integer> setTimeToNextSubjob(Integer machine, Integer time, ArrayList<Integer>timeToNextSubjob){
        timeToNextSubjob.set(machine, time);
        return timeToNextSubjob;
    }
    
	public static boolean largerThantheRandomNum(double temperature, Integer oldTime, Integer newTime) {
		double x = Math.random();
		Integer delta = oldTime - newTime;
		double y = Math.pow(Math.E, (delta / temperature));
		if (x < y) {
			return true;
		} else {
			return false;
		}
	}
}

class Job{
    public static ArrayList<Job> jobs = new ArrayList<Job>();

    public Integer jobID;
    public ArrayList<Subjob> subjobs;
    public Job(Integer jobID, ArrayList<Subjob> subjobs) {
        this.jobID = jobID;
        this.subjobs = subjobs;
    }

    public static Integer numOfMachinesRequired(ArrayList<Job> jobs){
        Integer numOfMachinesRequired = -1;
        for(Integer j = 0; j < jobs.size(); j++)
            for(Integer s = 0; s < jobs.get(j).subjobs.size(); s++)
                if(numOfMachinesRequired < jobs.get(j).subjobs.get(s).machine+1)
                    numOfMachinesRequired = jobs.get(j).subjobs.get(s).machine+1;
        return numOfMachinesRequired;
    }
}

class Subjob{
    public Integer jobID;
    public Integer orderInJob;
    public Integer machine;
    public Integer time;
 
    public Subjob(Integer jobID, Integer orderInJob, Integer machine, Integer time){
        this.jobID = jobID;
        this.orderInJob = orderInJob;
        this.machine = machine;
        this.time = time;
    }
}
class Queue{
    public ArrayList<Subjob> permutation;

    public Queue(ArrayList<Subjob> permutation){
        this.permutation = permutation;
    }

    public static ArrayList<Subjob> permOfM(Integer m, ArrayList<Queue>allQueues){
        return allQueues.get(m).permutation;
    }

    public static Subjob subjobInAllQueues(Integer m, Integer posInPerm, ArrayList<Queue>allQueues){
        return allQueues.get(m).permutation.get(posInPerm);
    }

    public static ArrayList<Queue> initalizeQueues (ArrayList<Job> jobs){
        Integer numOfMachines = Job.numOfMachinesRequired(jobs);
        ArrayList<Queue> queues = new ArrayList<Queue>();

        for(Integer m = 0; m<numOfMachines; m++)
            queues.add(new Queue(new ArrayList<Subjob>()));

        for(Integer j=0; j<jobs.size(); j++){
            Job job = jobs.get(j);
            for(Integer s = 0; s<job.subjobs.size(); s++){
                Subjob sj = job.subjobs.get(s);
                queues.get(sj.machine).permutation.add(sj);
            }
        }
        return queues;
    }

    public static ArrayList<Queue> neighbor(ArrayList<Queue> queues, Integer rounds){
        Random ranNum = new Random();
        for(Integer i = 0; i < rounds; i++){
            Integer m = ranNum.nextInt(queues.size());
            Queue q = queues.get(m);
            ranNum.nextInt(q.permutation.size());
            Integer index1 = ranNum.nextInt(q.permutation.size());
            Integer index2 = ranNum.nextInt(q.permutation.size());
            //System.out.println("index1: "+index1+"; index2= "+index2);
            Subjob sj1 = q.permutation.get(index1);
            Subjob sj2 = q.permutation.get(index2);
            q.permutation.set(index1, sj2);
            q.permutation.set(index2, sj1);
        }
        return queues;
    }
    
    public static void printQueues(ArrayList<Queue> queues){
        for(Integer m = 0; m < queues.size(); m++){
            System.out.print("M" + m +":\t");
            ArrayList<Subjob> subjobList = queues.get(m).permutation;
            for(Integer s = 0; s < subjobList.size(); s++)
                System.out.print(subjobList.get(s).jobID +"\t");
            System.out.println();
        }
    }
}
