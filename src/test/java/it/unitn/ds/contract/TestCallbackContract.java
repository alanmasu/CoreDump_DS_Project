package it.unitn.ds.contract;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractReplica;
import java.util.Collection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The automated tests detect what our system does only through the {@code callback*}
 * methods of {@link AbstractClient} and {@link AbstractReplica}. Forgetting to invoke one
 * makes a correct implementation look broken, and nothing else in the build notices.
 * <p>
 * The check is static: it reads the compiled bytecode and looks for the call, so it
 * catches "you never call it", not "you call it at the wrong moment".
 * <p>
 * It asks whether <em>anything</em> in {@code it.unitn.ds} makes the call, rather than
 * pinning it on Client or Replica. A callback is likely to be fired from a Transaction
 * subclass rather than from the actor itself, and ArchUnit reports only the calls found in
 * a class's own bytecode — no transitive reachability — so a rule scoped to the actor
 * would stay red no matter how correct the implementation is. Scoping to the package
 * costs nothing: the callbacks are package-private, so nobody else could call them.
 */
@Tag("contract")
class TestCallbackContract {

    private static Collection<JavaClass> ourClasses;

    @BeforeAll
    static void importClasses() {
        ourClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("it.unitn.ds");
    }

    /** Fails unless some class under it.unitn.ds invokes {@code owner.callbackName(..)}. */
    private static void someoneMustInvoke(Class<?> owner, String callbackName) {
        boolean called = ourClasses.stream()
                .flatMap(javaClass -> javaClass.getMethodCallsFromSelf().stream())
                .anyMatch(call -> call.getTargetOwner().isAssignableTo(owner)
                        && call.getName().equals(callbackName));

        assertTrue(
                called,
                String.format(
                        "%s.%s(..) is never invoked anywhere in it.unitn.ds; the automated tests "
                                + "observe this event only through that call",
                        owner.getSimpleName(), callbackName));
    }

    // --- Client -------------------------------------------------------------

    @Test
    void clientReportsReadResults() {
        someoneMustInvoke(AbstractClient.class, "callbackOnReadResult");
    }

    @Test
    void clientReportsWriteResults() {
        someoneMustInvoke(AbstractClient.class, "callbackOnWriteResult");
    }

    @Test
    void clientReportsReadTimeouts() {
        someoneMustInvoke(AbstractClient.class, "callbackOnReadTimeout");
    }

    @Test
    void clientReportsWriteTimeouts() {
        someoneMustInvoke(AbstractClient.class, "callbackOnWriteTimeout");
    }

    // --- Replica ------------------------------------------------------------

    @Test
    void replicaReportsAppliedUpdates() {
        someoneMustInvoke(AbstractReplica.class, "callbackOnUpdateApplied");
    }

    @Test
    void replicaReportsStartedElections() {
        someoneMustInvoke(AbstractReplica.class, "callbackOnElectionStarted");
    }

    @Test
    void replicaReportsElectedCoordinators() {
        someoneMustInvoke(AbstractReplica.class, "callbackOnCoordinatorElected");
    }
}
