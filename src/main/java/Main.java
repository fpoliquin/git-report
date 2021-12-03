import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Optional;
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

        exportRepo(tempPath, repoName, database);
    }

    private static void exportRepo(final Path tempPath, final String repoName, CommitDatabase database) throws Exception {
        Repository repository = openRepository(tempPath);
		
        long snapshotId = database.createSnapshot(repoName);

        saveBranches(database, repoName, snapshotId, findBranchesSortedByDate(repository));
        
        saveReleases(repository, database, repoName);

        saveCommits(repository, database, repoName);
    }

    static void saveCommits(Repository repo, CommitDatabase database, String repoName) throws Exception {
        List<Ref> branches = findBranchesSortedByDate(repo);
        List<Ref> releases = findReleasesSortedByDate(repo);
        List<RevCommit> alreadyHandledCommits = new ArrayList<>();

        try (RevWalk walk = new RevWalk(repo)) {
            saveCommits(database, repoName, releases, alreadyHandledCommits, walk, true);
            saveCommits(database, repoName, branches, alreadyHandledCommits, walk, false);
        }
    }

    private static void saveCommits(CommitDatabase database, String repoName, List<Ref> roots, List<RevCommit> alreadyHandledCommits, RevWalk walk,
            boolean rootsAreReleases)
            throws IOException {
        ArrayList<Ref> rootsToProcess = new ArrayList<>(roots);

        while (!rootsToProcess.isEmpty()) {
            final Ref root = rootsToProcess.get(0);

            // System.out.println(root.getName());

            final RevCommit currentCommit = walk.parseCommit(root.getObjectId());

            final Optional<Long> currentCommitTimestamp = findCommitTimestamp(currentCommit);

            walk.markStart(currentCommit);

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
                        root.getName(),
                        rootsAreReleases ? currentCommitTimestamp : null,
                        parents);
                ++count;
            }

            rootsToProcess.remove(0);
            alreadyHandledCommits.add(currentCommit);
            walk.reset();

            System.out.println("Saved " + count + " commits");
        }
    }

    static Optional<Long> findCommitTimestamp(RevCommit commit) {
        try {
            return Optional.of(commit.getAuthorIdent().getWhen().getTime());
        } catch (NullPointerException e) {
            // Il y a un bug avec le commit 31f3d296eb1d0d50ed056c01c54dd60ea7bf62d7 dans mpo-ui
            // java.lang.NullPointerException at org.eclipse.jgit.util.RawParseUtils.author(RawParseUtils.java:726)
            return Optional.empty();
        }
    }

    static Repository openRepository(Path path) throws IOException {
        FileRepositoryBuilder builder = new FileRepositoryBuilder();
        return builder.setGitDir(path.toFile())
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
            RevCommit commit = repository.parseCommit(release.getObjectId());
            database.saveRelease(repoName, release.getName(), commit.getAuthorIdent().getWhen().getTime(), release.getObjectId().getName());
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

        void saveCommit(String repoName, String hash, long commitTimestamp, String releaseName, Optional<Long> releaseTimestamp, Set<String> parents);

        void saveBranch(String repoName, long snapshotId, String branchName, String commitHash);

        void saveRelease(String repoName, String releaseName, long releaseTimestamp, String commitHash);
    }

    static class DummyCommitDatabase implements CommitDatabase {
        @Override
        public long createSnapshot(String repoName) {
            return 1;
        }

        @Override
        public void saveCommit(String repoName, String hash, long commitTimestamp, String releaseName, Optional<Long> releaseTimestamp, Set<String> parents) {

            System.out.println("Commit " + hash + ": " + releaseName);
        }

        @Override
        public void saveBranch(String repoName, long snapshotId, String branchName, String commitHash) {

        }

        @Override
        public void saveRelease(String repoName, String releaseName, long releaseTimestamp, String commitHash) {

        }
    }
}
