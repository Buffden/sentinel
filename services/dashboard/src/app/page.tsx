import TopNav from '@/widgets/top-nav/TopNav'
import Footer from '@/shell/Footer'
import Workspace from '@/workspace/Workspace'

export default function Page() {
	return (
		<div style={{ display: 'flex', flexDirection: 'column', height: '100vh', width: '100vw', overflow: 'hidden' }}>
			<TopNav />
			<Workspace />
			<Footer />
		</div>
	)
}
