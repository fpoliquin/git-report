import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;

public class Main {

	public static void main(String[] args) throws Exception {
        final Path tempPath = Path.of("target", "checkout");
        // final Path tempPath = Path.of("C:\\Dev\\src\\campus\\mpo\\mpo-ui\\.git");
        final String repoName = "mpo-ui";

        if (Files.notExists(tempPath)) {
            throw new RuntimeException("The path " + tempPath + " does not exist.");
        }

        CommitDatabase database = new DummyCommitDatabase();

        Repository repository = openRepository(tempPath);
		
        long snapshotId = database.createSnapshot(repoName);

        // saveBranches(database, repoName, snapshotId, branches(repository));
        
        // saveReleases(repository, database, repoName);

        Ref master = repository.getRefDatabase().exactRef("refs/heads/master");
        saveCommits(repository, database, repoName);
    }

    static void saveCommits(Repository repo, CommitDatabase database, String repoName) throws Exception {
        List<Ref> branches = findBranchesSortedByDate(repo);
        List<Ref> releases = findReleasesSortedByDate(repo);
        List<RevCommit> alreadyHandledCommits = new ArrayList<>();

        try (RevWalk walk = new RevWalk(repo)) {
            while (!releases.isEmpty()) {
                final Ref release = releases.get(0);
                final RevCommit currentReleaseCommit = walk.parseCommit(release.getObjectId());
                final long releaseTimestamp = currentReleaseCommit.getAuthorIdent().getWhen().getTime();

                walk.markStart(currentReleaseCommit);
                for (RevCommit alreadyHandledCommit : alreadyHandledCommits) {
                    walk.markUninteresting(alreadyHandledCommit);
                }

                RevCommit commit;
                long count = 0;

                while ((commit = walk.next()) != null) {
                    Set<String> parents = Arrays.stream(commit.getParents()).map(parent -> parent.getName()).collect(Collectors.toSet());
                    database.saveCommit(repoName,
                            commit.getName(),
                            commit.getAuthorIdent().getWhen().getTime(),
                            release.getName(),
                            releaseTimestamp, parents);
                    ++count;
                }

                releases.remove(0);
                alreadyHandledCommits.add(currentReleaseCommit);

                System.out.println("Saved " + count + " commits");
            }
        }
    }

    static Repository openRepository(Path path) throws IOException {
        FileRepositoryBuilder builder = new FileRepositoryBuilder();
        return builder.setGitDir(path.toFile())
                .findGitDir()
                // .setBare()
                .build();
    }

    static void saveBranches(CommitDatabase database, String repoName, long snapshotId, List<Ref> branches) throws Exception {
        for (Ref branch : branches) {
            System.out.println(branch.getName());
            database.saveBranch(repoName, snapshotId, branch.getName(), branch.getObjectId().getName());
        }
    }

    static void saveReleases(Repository repository, CommitDatabase database, String repoName) throws Exception {
        for (Ref release : findReleasesSortedByDate(repository)) {
            System.out.println(release.getName());
            database.saveRelease(repoName, release.getName(), release.getObjectId().getName());
        }
    }

    static List<Ref> findReleasesSortedByDate(Repository repository) throws Exception {
        return findTagsSortedByDate(repository).stream()
                .filter(ref -> ref.getName().matches(".+@[\\d\\.]+$"))
                .collect(Collectors.toList());
    }

    static List<Ref> findBranchesSortedByDate(Repository repository) throws Exception {
        return repository.getRefDatabase().getRefsByPrefix(Constants.R_HEADS).stream()
                .sorted(new SortByCommitDateComparator(repository))
                .collect(Collectors.toList());
	}

    static List<Ref> findTagsSortedByDate(Repository repository) throws Exception {
        return repository.getRefDatabase().getRefsByPrefix(Constants.R_TAGS).stream()
                .sorted(new SortByCommitDateComparator(repository))
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

        void saveCommit(String repoName, String hash, long commitTimestamp, String releaseName, long releaseTimestamp, Set<String> parents);

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
        public void saveCommit(String repoName, String hash, long commitTimestamp, String releaseName, long releaseTimestamp,
                Set<String> parents) {

            System.out.println("Commit " + hash + ": " + releaseName);
        }

        @Override
        public void saveBranch(String repoName, long snapshotId, String branchName, String commitHash) {

        }

        @Override
        public void saveRelease(String repoName, String releaseName, String commitHash) {

        }
    }
}
