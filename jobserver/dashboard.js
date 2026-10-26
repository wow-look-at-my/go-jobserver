// The dashboard's graph: js-snippets' <dag-view>, fed from /api/graph. Without it the tables still say everything.
const view = document.getElementById('graph');
if (view) {
	import('https://sites.pazer.build/js-snippets/branch/library/ui/dag-view.js').then(async () => {
		const g = (await (await fetch('/api/graph')).json()).graph;
		view.setData({
			nodes: g.nodes.map(n => ({
				id: n.id, label: n.label, state: n.state, category: n.kind,
				detail: [n.status, ...(n.waiting || [])].filter(Boolean).join('\n'),
				href: '/job/' + encodeURIComponent(n.id),
			})),
			edges: g.edges.map(e => ({from: e.from, to: e.to, label: e.label})),
		});
	}).catch(err => {
		view.replaceWith(Object.assign(document.createElement('p'), {className: 'err', textContent: 'graph view failed to load: ' + err}));
	});
}
