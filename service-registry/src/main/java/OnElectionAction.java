import cluster.management.OnElectionCallback;
import cluster.management.ServiceRegistry;
import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class OnElectionAction implements OnElectionCallback {
    private final ServiceRegistry serviceRegistry;
    private final int port;
    // 이 클래스는 레지스트리에 대한 참조값, 앱 주소에 포함될 포트#를 가지고 있음

    public OnElectionAction(ServiceRegistry serviceRegistry, int port) {
        this.serviceRegistry = serviceRegistry;
        this.port = port;
        // 이런 매개변수는 생성자의 인수를 통해 클래스에 전달됨
    }

    @Override
    public void onElectedToBeLeader() {
        serviceRegistry.unregisterFromCluster();
        // 리더로 선출될 시 unregisterFromCluster()를 먼저 호출!
        // 노드가 클러스터에 합류한 경우 이 메서드는 아무 작업도 수행하지 않지만 이전에 워커 노드였다가
        // 리더 노드가 된 경우에는 레지스트리에서 자신을 제거함!
        serviceRegistry.registerForUpdates();
    }

    @Override
    public void onWorker() {
        try {
            String currentServerAddress =
                    String.format("http://%s:%d", InetAddress.getLocalHost().getCanonicalHostName(), port);
          // 로컬 호스트명과 포트를 하나의 문자열로 결합해서 서버주소를 만듦!
            serviceRegistry.registerToCluster(currentServerAddress);
            // 위 주소를 메타데이터로 변환하여 다른 노드가 registerToCluster()를 호출해서 보도록 저장!
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
