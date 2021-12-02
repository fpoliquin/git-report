import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;

public class Main {

	public static void main(String[] args) throws Exception {
        final Path tempPath = Path.of("target", "checkout");
        final String repoName = "mpo-ui";

        CommitDatabase database = new DummyCommitDatabase();

		FileRepositoryBuilder builder = new FileRepositoryBuilder();
		Repository repository = builder.setGitDir(tempPath.toFile()).findGitDir().setBare().build();
		
        Ref master = repository.getRefDatabase().exactRef("refs/heads/master");

        RevCommit commit = repository.parseCommit(master.getObjectId());

        System.out.println(commit.getName());
        System.out.println(commit.getAuthorIdent());
        System.out.println(commit.getCommitTime());
        System.out.println(commit.getParents());

        long snapshotId = database.createSnapshot(repoName);

        saveBranches(database, repoName, snapshotId, branches(repository));
        
        saveReleases(repository, database, repoName);
    }

    static void saveBranches(CommitDatabase database, String repoName, long snapshotId, List<Ref> branches) throws Exception {
        for (Ref branch : branches) {
            System.out.println(branch.getName());
            database.saveBranch(repoName, snapshotId, branch.getName(), branch.getObjectId().getName());
        }
    }

    static void saveReleases(Repository repository, CommitDatabase database, String repoName) throws Exception {
        for (Ref release : releases(repository)) {
            System.out.println(release.getName());
            database.saveRelease(repoName, release.getName(), release.getObjectId().getName());
        }
    }

    static List<Ref> releases(Repository repository) throws Exception {
        return tags(repository).stream()
                .filter(ref -> ref.getName().matches(".+@[\\d\\.]+$"))
                .collect(Collectors.toList());
    }

    static List<Ref> branches(Repository repository) throws Exception {
        return repository.getRefDatabase().getRefsByPrefix(Constants.R_HEADS).stream()
                .sorted(new SortByCommitDateComparator(repository))
                // .map(ref -> ref.getName().substring(Constants.R_HEADS.length()))
                .collect(Collectors.toList());
	}

    static List<Ref> tags(Repository repository) throws Exception {
        return repository.getRefDatabase().getRefsByPrefix(Constants.R_TAGS).stream()
                .sorted(new SortByCommitDateComparator(repository))
                // .map(ref -> ref.getName().substring(Constants.R_TAGS.length()))
                .collect(Collectors.toList());
    }

    static class SortByCommitDateComparator implements Comparator<Ref> {
        private final Repository repo;

        SortByCommitDateComparator(Repository repo) {
            this.repo = repo;
        }

        @Override
        public int compare(Ref ref1, Ref ref2) {
            return findDate(ref1).compareTo(findDate(ref2));
        }

        Date findDate(Ref ref) {
            try {
                return repo.parseCommit(ref.getObjectId()).getAuthorIdent().getWhen();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    interface CommitDatabase {
        long createSnapshot(String repoName);

        void saveCommit(String repoName, String hash, long commitTimestamp, List<String> parents);

        void markCommitAsReleased(String repoName, String hash, long releaseTimestamp);

        void saveBranch(String repoName, long snapshotId, String branchName, String commitHash);

        void saveRelease(String repoName, String releaseName, String commitHash);
    }

    static class DummyCommitDatabase implements CommitDatabase {
        @Override
        public
        long createSnapshot(String repoName) {
            return 1;
        }

        @Override
        public void saveCommit(String repoName, String hash, long commitTimestamp, List<String> parents) {

        }

        @Override
        public void markCommitAsReleased(String repoName, String hash, long releaseTimestamp) {

        }

        @Override
        public void saveBranch(String repoName, long snapshotId, String branchName, String commitHash) {

        }

        @Override
        public void saveRelease(String repoName, String releaseName, String commitHash) {

        }
    }
}
