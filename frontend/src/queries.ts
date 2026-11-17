import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { useEffect } from "react"

import type { DepType, Issue, UpdatePayload } from "@/api"
import {
  addComment,
  addDep,
  fetchActors,
  fetchComments,
  fetchGraph,
  fetchIssue,
  fetchIssues,
  fetchReady,
  updateIssue,
} from "@/api"
import {
  type CommentIndex,
  type Filters,
  scopeNeedsComments,
} from "@/lib/filter"

// The server pushes a change event the moment its data moves (see
// useLiveUpdates), so polling is only a safety net for a dropped stream.
const REFETCH_MS = 30_000

// useLiveUpdates follows the server's change stream and refetches the active
// queries whenever the data changes. EventSource reconnects by itself; after a
// reconnect everything is refetched, since changes may have been missed.
export function useLiveUpdates() {
  const qc = useQueryClient()

  useEffect(() => {
    const es = new EventSource("/api/stream")
    let seen = ""
    let connected = false

    // Each event carries the server's data generation; repeats are skipped.
    const refresh = (gen: string) => {
      if (gen === seen) {
        return
      }

      seen = gen
      void qc.invalidateQueries()
    }

    es.addEventListener("hello", (e) => {
      // A restarted server counts generations from scratch, so a reconnect
      // refetches regardless of the number it reports.
      seen = String(e.data)

      if (connected) {
        void qc.invalidateQueries()
      }

      connected = true
    })
    es.addEventListener("change", (e) => {
      refresh(String(e.data))
    })

    return () => {
      es.close()
    }
  }, [qc])
}

export function useIssues() {
  return useQuery({
    queryKey: ["issues"],
    queryFn: fetchIssues,
    refetchInterval: REFETCH_MS,
  })
}

// useCommentIndex fetches the comment text that comment-scoped search reads.
// The fetch is a full export, so it runs only while such a search is active.
export function useCommentIndex(filters: Filters): CommentIndex | undefined {
  const { data } = useQuery({
    queryKey: ["comments"],
    queryFn: fetchComments,
    enabled: scopeNeedsComments(filters),
    refetchInterval: REFETCH_MS,
  })

  return data
}

export function useReady() {
  return useQuery({
    queryKey: ["ready"],
    queryFn: fetchReady,
    refetchInterval: REFETCH_MS,
  })
}

export function useActors() {
  return useQuery({
    queryKey: ["actors"],
    queryFn: fetchActors,
    staleTime: 30 * 60_000,
  })
}

export function useGraph() {
  return useQuery({
    queryKey: ["graph"],
    queryFn: fetchGraph,
    refetchInterval: REFETCH_MS,
  })
}

export function useIssue(id: string) {
  return useQuery({
    queryKey: ["issue", id],
    queryFn: async () => await fetchIssue(id),
    enabled: id !== "",
  })
}

// useIssueWrites returns the drawer mutations. On success each seeds the fresh
// issue into the detail cache and invalidates the list/ready/graph views so
// every tab reflects the change without a manual reload.
function useIssueWrites() {
  const qc = useQueryClient()

  const settle = (fresh: Issue | undefined, id: string) => {
    if (fresh) {
      qc.setQueryData(["issue", id], fresh)
    }

    void qc.invalidateQueries({ queryKey: ["issues"] })
    void qc.invalidateQueries({ queryKey: ["ready"] })
    void qc.invalidateQueries({ queryKey: ["graph"] })
    void qc.invalidateQueries({ queryKey: ["issue", id] })
    void qc.invalidateQueries({ queryKey: ["comments"] })
  }

  return { settle }
}

export function useUpdateIssue() {
  const { settle } = useIssueWrites()

  return useMutation({
    mutationFn: async (v: {
      id: string
      actor: string
      patch: UpdatePayload
    }) => await updateIssue(v.id, v.actor, v.patch),
    onSuccess: (fresh, v) => {
      settle(fresh, v.id)
    },
  })
}

export function useAddComment() {
  const { settle } = useIssueWrites()

  return useMutation({
    mutationFn: async (v: { id: string; actor: string; text: string }) =>
      await addComment(v.id, v.actor, v.text),
    onSuccess: (fresh, v) => {
      settle(fresh, v.id)
    },
  })
}

export function useAddDep() {
  const { settle } = useIssueWrites()

  return useMutation({
    mutationFn: async (v: {
      id: string
      actor: string
      dependsOn: string
      type: DepType
    }) => await addDep(v.id, v.actor, v.dependsOn, v.type),
    onSuccess: (fresh, v) => {
      settle(fresh, v.id)
    },
  })
}
