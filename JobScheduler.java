import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

class Job implements Comparable<Job> {
    String jobName;
    int executionTime;
    List<String> requiredResources;
    List<String> dependencies;
    int importance;
    long startTime;
    
    Job(String jobName, int executionTime, List<String> requiredResources, List<String> dependencies, int importance) {
        this.jobName = jobName;
        this.executionTime = executionTime;
        this.requiredResources = requiredResources;
        this.dependencies = dependencies;
        this.importance = importance;
    }
    
    @Override
    public int compareTo(Job other) {
        return Integer.compare(other.importance, this.importance);
    }
}

class ResourceManager {
    private final Set<String> availableResources;
    private final ReentrantLock lock = new ReentrantLock();

    ResourceManager(List<String> initialResources) {
        this.availableResources = new HashSet<>(initialResources);
    }

    boolean allocateResources(List<String> requiredResources) {
        lock.lock();
        try {
            if (availableResources.containsAll(requiredResources)) {
                availableResources.removeAll(requiredResources);
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    void releaseResources(List<String> requiredResources) {
        lock.lock();
        try {
            availableResources.addAll(requiredResources);
        } finally {
            lock.unlock();
        }
    }
}

public class JobScheduler {
    private final List<Job> jobs;
    private final ResourceManager resourceManager;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final Map<String, CompletableFuture<Void>> jobCompletionMap = new HashMap<>();
    
    JobScheduler(List<Job> jobs, List<String> initialResources) {
        this.jobs = new ArrayList<>(jobs);
        this.resourceManager = new ResourceManager(initialResources);
    }

    public void schedule() {
        long startTime = System.currentTimeMillis();
        PriorityQueue<Job> jobQueue = new PriorityQueue<>(jobs);

        while (!jobQueue.isEmpty()) {
            Job job = jobQueue.poll();
            CompletableFuture<Void> dependenciesFuture = CompletableFuture.allOf(
                    job.dependencies.stream()
                            .map(dep -> jobCompletionMap.getOrDefault(dep, CompletableFuture.completedFuture(null)))
                            .toArray(CompletableFuture[]::new)
            );

            dependenciesFuture.thenRunAsync(() -> {
                while (!resourceManager.allocateResources(job.requiredResources)) {
                    try {
                        Thread.sleep(100);  // Wait and retry resource allocation
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                
                job.startTime = System.currentTimeMillis() - startTime;
                System.out.println("Job " + job.jobName + " started at " + job.startTime + " seconds using resources " + job.requiredResources);
                
                try {
                    Thread.sleep(job.executionTime * 1000);  // Simulate job execution
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    resourceManager.releaseResources(job.requiredResources);
                    jobCompletionMap.get(job.jobName).complete(null);
                }
            }, executorService);
            
            jobCompletionMap.put(job.jobName, dependenciesFuture);
        }
    }

    public static void main(String[] args) {
        List<Job> jobs = List.of(
                new Job("Job1", 5, List.of("CPU"), List.of(), 3),
                new Job("Job2", 3, List.of("GPU"), List.of("Job1"), 2),
                new Job("Job3", 2, List.of("CPU", "GPU"), List.of("Job2"), 1)
        );
        List<String> resources = List.of("CPU", "GPU");

        JobScheduler scheduler = new JobScheduler(jobs, resources);
        scheduler.schedule();
    }
}
