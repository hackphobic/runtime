use std::collections::{HashMap, HashSet, VecDeque};

use crate::service::{ServiceName, ServiceSpec};

#[derive(thiserror::Error, Debug)]
pub enum DagError {
    #[error("unknown dependency {dep} for service {service}")]
    UnknownDependency { service: ServiceName, dep: ServiceName },

    #[error("cycle detected in dependency graph involving: {0:?}")]
    Cycle(Vec<ServiceName>),
}

/// Topologically sort services based on declared dependencies.
/// Returns ordered list of indices into `specs`.
pub fn topo_sort(specs: &[ServiceSpec]) -> Result<Vec<usize>, DagError> {
    let mut name_to_idx: HashMap<ServiceName, usize> = HashMap::new();
    for (i, s) in specs.iter().enumerate() {
        name_to_idx.insert(s.name, i);
    }

    // Validate deps exist
    for s in specs {
        for &d in &s.deps {
            if !name_to_idx.contains_key(&d) {
                return Err(DagError::UnknownDependency {
                    service: s.name,
                    dep: d,
                });
            }
        }
    }

    // Kahn's algorithm
    let mut indegree = vec![0usize; specs.len()];
    let mut outgoing: Vec<Vec<usize>> = vec![Vec::new(); specs.len()];

    for (i, s) in specs.iter().enumerate() {
        for &dep_name in &s.deps {
            let dep_idx = name_to_idx[&dep_name];
            indegree[i] += 1;
            outgoing[dep_idx].push(i);
        }
    }

    let mut q = VecDeque::new();
    for (i, &d) in indegree.iter().enumerate() {
        if d == 0 {
            q.push_back(i);
        }
    }

    let mut out = Vec::with_capacity(specs.len());
    while let Some(i) = q.pop_front() {
        out.push(i);
        for &n in &outgoing[i] {
            indegree[n] -= 1;
            if indegree[n] == 0 {
                q.push_back(n);
            }
        }
    }

    if out.len() != specs.len() {
        // cycle: collect remaining
        let mut remaining = Vec::new();
        for (i, d) in indegree.iter().enumerate() {
            if *d > 0 {
                remaining.push(specs[i].name);
            }
        }
        // Dedup in case
        let mut set = HashSet::new();
        remaining.retain(|x| set.insert(*x));
        return Err(DagError::Cycle(remaining));
    }

    Ok(out)
}