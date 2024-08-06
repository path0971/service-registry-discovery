package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {
    /* 이벤트 알림을 받으려면 먼저 Watcher 인터페이스를 상속받고
     * 이벤트 처리를 위한 process() 메소드를 정의해야 함!
     */
    private static final String REGISTRY_ZNODE = "/service_registry";
    // 레지스트리 Znode의 이름 초기화 ==> 서비스 레지스트리의 부모 Znode
    private final ZooKeeper zooKeeper;
    private String currentZnode = null;
    // 생성하려는 Znode를 저장할 멤버변수 생성!
    private List<String> allServiceAddresses = null;
    // 클러스터 내 모든 노드의 Cache를 저장할 List 멤버변수 생성!

    public ServiceRegistry(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        createServiceRegistryZnode();
        /* 생성자에서 메소드를 호출하면 ServiceRegistry 객체를 생성한 모든 노드가 
           서비스 레지스트리 Znode가 있는 지 확인 후 없으면 생성함!
         */
    }

    public void registerToCluster(String metadata) throws KeeperException, InterruptedException {
       /* 이 메타데이터엔 클러스터와 공유하려는 어떤 설정값도 넣을 수 있음!
        * 우선, 노드 주소만 넣을 겁니당
        */
        if (this.currentZnode != null) {
            System.out.println("Already registered to service registry");
            return;
        }
        this.currentZnode = zooKeeper.create(REGISTRY_ZNODE + "/n_", metadata.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        /* 부모 Znode 아래에 있는 Znode의 경로를 인자값으로 create() 메소드를 호출함
         * 이후, Znode에 저장할 메타데이터를 바이너리 형태로 전달함, Znode는 이 앱의 
         * 특정 인스턴스와 lifecycle을 함께하기 위해 '순서'를 가진 임시 노드로 생성함
        */        
        System.out.println("Registered to service registry");
        // 레지스트리 등록 확인 메시지!
    }

    public void registerForUpdates() {
        try {
            updateAddresses();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void createServiceRegistryZnode() {
        try {
            if (zooKeeper.exists(REGISTRY_ZNODE, false) == null) {
                zooKeeper.create(REGISTRY_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            /* 가장 먼저 할 일은 레지스트리의 Znode를 생성 or 이미 존재하는 지 확인!
             * exists() 메소드 호출을 통해 반환값을 확인하고 Znode 유무를 파악!
             * 이 값이 null이면 Znode가 없는 것이므로 영구 모드를 통해 create() 메소드를 호출!
             */
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized List<String> getAllServiceAddresses() throws KeeperException, InterruptedException {
        /* 리더 노드가 업뎃을 등록한 이후, 최신 주소 목록이 필요할 수 있으므로
         * 동기식으로 이 메소드를 만듦
         */
        if (allServiceAddresses == null) {
            updateAddresses();
        }
        return allServiceAddresses;
        /* allServiceAddresses의 값이 null이면 호출한 쪽에서 업데이트 등록을 잊은 것이므로 
        반환값이 비어 있지 않도록 처음엔 updateAddresses() 메서드를 호출하고, 처음이 아니면
        allServiceAddresses 변수를 반환! */
    }

    public void unregisterFromCluster() {
        // 먼저 레지스트리에 등록된 Znode가 존재하는 지 확인 후, 존재 시 delete() 메소드를 호출하여 삭제!
        try {
            if (currentZnode != null && zooKeeper.exists(currentZnode, false) != null) {
                zooKeeper.delete(currentZnode, -1);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private synchronized void updateAddresses() throws KeeperException, InterruptedException {
        // synchronized 타입을 통해 여러 스레드에서 호출될 수 있게 지정!
        List<String> workerZnodes = zooKeeper.getChildren(REGISTRY_ZNODE, this);
        // 현재의 자식 node목록을 가져오고 목록 변경 이벤트도 등록 가능케 함!
        List<String> addresses = new ArrayList<>(workerZnodes.size());
        // 모든 클러스터의 주소를 저장할 임시 목록을 만들어 줌

        for (String workerZnode : workerZnodes) {
            /* 모든 자식 Znode에 대해 각 Znode의 전체 경로를 생성하고 
             * exists() 메소드를 호출하여 Znode의 상태정보를 가져오는 일을 반복하고,
             * 이를 통해 Znode의 데이터를 가져오기 위한 사전작업을 정의한 것!
             */
            String workerFullPath = REGISTRY_ZNODE + "/" + workerZnode;
            Stat stat = zooKeeper.exists(workerFullPath, false);
            if (stat == null) {
                continue;
            }
            /* 만약 getChildren() 메소드 호출과 exists() 호출 사이에 자식 Znode가 사라진다면?
             * 메소드의 반환값이 null이 될 수 있당
             * ==> 멀티 스레딩 상황에서 주의해야 한당!!
             */

            byte[] addressBytes = zooKeeper.getData(workerFullPath, false, stat);
            // 만약 Znode가 존재한다면? 해당 Znode의 경로에 대해 getData() 메소드를 호출!
            String address = new String(addressBytes);
            addresses.add(address);
            // 그런 다음 바이너리값으로 문자열로 변환하여 주소 목록에 추가!
        }

        this.allServiceAddresses = Collections.unmodifiableList(addresses);
        /* 서비스 레지스트리에서 이전 주소 목록을 가져오면 이 목록을 변경불가한 List 타입으로
         * 만들어 allServiceAddress 필드에 저장!
         * 이 메소드는 동기형식으로 동작하므로 전체적인 업뎃은 자동적으로 수행한당!
         */
        System.out.println("The cluster addresses are: " + this.allServiceAddresses);
        // 주소 출력맨
    }

    @Override
    public void process(WatchedEvent event) {
        /* process()를 통해 해당 이벤트를 처리해줌
         * updateAddress() 메소드를 다시 호출해주면 되는 것!
         * 이를 통해 allServiceAddress 변수가 업뎃되고 이후에도 여부를 알 수 있음!
         * 추가해야할 부분은 updateAddress()에 대한 첫 호출로, 이를 호출하는 registerForUpdates()를 만들어주자!  
         */
        try {
            updateAddresses();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
