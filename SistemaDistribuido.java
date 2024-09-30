import java.util.Random;
import java.util.concurrent.*;

public class SistemaDistribuido {

    private static final int NUMERO_DE_NOS = 4;
    private static final int NUMERO_DE_CLIENTES = 3;
    private static final int INTERVALO_RESULTADO = 30_000;

    
    private static BlockingQueue<Tarefa> filaTarefas = new LinkedBlockingQueue<>();
    
    
    private static ConcurrentHashMap<Integer, String> resultados = new ConcurrentHashMap<>();

    
    static class Tarefa {
        private final int id;

        public Tarefa(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public String processar() throws InterruptedException {
        
            Thread.sleep(new Random().nextInt(5000));
            return "Resultado da tarefa " + id;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executor = Executors.newCachedThreadPool();

        
        for (int i = 0; i < NUMERO_DE_CLIENTES; i++) {
            int clientId = i + 1;
            executor.submit(() -> enviarTarefas(clientId));
        }

        
        for (int i = 0; i < NUMERO_DE_NOS; i++) {
            executor.submit(SistemaDistribuido::processarTarefas);
        }

        
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(SistemaDistribuido::exibirResultados, 0, 30, TimeUnit.SECONDS);

        
        TimeUnit.MINUTES.sleep(2);
        executor.shutdown();
        scheduler.shutdown();

        if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
            System.out.println("ExecutorService não finalizou a tempo. Finalizando forçadamente.");
            executor.shutdownNow();  
        }

        if (!scheduler.awaitTermination(1, TimeUnit.MINUTES)) {
            System.out.println("ScheduledExecutorService não finalizou a tempo. Finalizando forçadamente.");
            scheduler.shutdownNow();
        }

        System.out.println("Programa finalizado com sucesso.");
    }

    
    private static void enviarTarefas(int clientId) {
        try {
            Random random = new Random();
            for (int i = 0; i < 10; i++) {
                int tarefaId = clientId * 100 + i;
                Tarefa tarefa = new Tarefa(tarefaId);
                System.out.println("Cliente " + clientId + " enviou tarefa " + tarefaId);
                filaTarefas.put(tarefa); 
                Thread.sleep(random.nextInt(2000)); 
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    
    private static void processarTarefas() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Tarefa tarefa = filaTarefas.take(); 
                String resultado = tarefa.processar(); 
                resultados.put(tarefa.getId(), resultado);
                System.out.println("Tarefa " + tarefa.getId() + " processada com sucesso.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    
    private static void exibirResultados() {
        System.out.println("\nResultados Processados:");
        resultados.forEach((id, resultado) -> System.out.println("Tarefa " + id + ": " + resultado));
    }
}
